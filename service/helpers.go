package service

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/viant/embedius/document"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/indexer/fs/splitter"
	"github.com/viant/embedius/matching"
	"github.com/viant/embedius/matching/option"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectordb/meta"
	"github.com/viant/sqlite-vec/vector"
)

type sqlQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type sqlDialect string

const (
	dialectSQLite sqlDialect = "sqlite"
	dialectMySQL  sqlDialect = "mysql"
)

func resolveDialect(driver string) sqlDialect {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "mysql", "mariadb":
		return dialectMySQL
	default:
		return dialectSQLite
	}
}

func localDocsTable(driver string) string {
	if resolveDialect(driver) == dialectMySQL {
		return "shadow_vec_docs"
	}
	return "_vec_emb_docs"
}

type fileItem struct {
	abs     string
	rel     string
	assetID string
	md5     string
	size    int64
	modTime time.Time
	data    []byte
}

type assetInfo struct {
	md5      string
	archived bool
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func hashMD5(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

func listFiles(root string, matcher *matching.Manager) ([]fileItem, error) {
	var out []fileItem
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if matcher != nil {
			size := info.Size()
			if size > int64(int(^uint(0)>>1)) {
				return nil
			}
			if matcher.IsExcluded(rel, int(size)) {
				return nil
			}
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		md5hex := hashMD5(data)
		out = append(out, fileItem{
			abs:     path,
			rel:     rel,
			assetID: rel,
			md5:     md5hex,
			size:    info.Size(),
			modTime: info.ModTime(),
			data:    data,
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return out, nil
}

func newMatcher(spec RootSpec) *matching.Manager {
	var opts []option.Option
	opts = append(opts, option.WithDefaultExclusionPatterns())
	if len(spec.Include) > 0 {
		opts = append(opts, option.WithInclusionPatterns(gitifyPatterns(spec.Include)...))
	}
	if len(spec.Exclude) > 0 {
		opts = append(opts, option.WithExclusionPatterns(gitifyPatterns(spec.Exclude)...))
	}
	if spec.MaxSizeBytes > 0 {
		if spec.MaxSizeBytes > int64(int(^uint(0)>>1)) {
			spec.MaxSizeBytes = int64(int(^uint(0) >> 1))
		}
		opts = append(opts, option.WithMaxIndexableSize(int(spec.MaxSizeBytes)))
	}
	return matching.New(opts...)
}

func gitifyPatterns(patterns []string) []string {
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		negated := strings.HasPrefix(pattern, "!")
		if negated {
			pattern = strings.TrimSpace(strings.TrimPrefix(pattern, "!"))
			if pattern == "" {
				continue
			}
		}
		if strings.HasPrefix(pattern, "git:") {
			if negated {
				out = append(out, "!"+pattern)
			} else {
				out = append(out, pattern)
			}
			continue
		}
		if negated {
			out = append(out, "!git:"+pattern)
		} else {
			out = append(out, "git:"+pattern)
		}
	}
	return out
}

func newSyncFilter(spec RootSpec) func(path string, meta string) bool {
	if len(spec.Include) == 0 && len(spec.Exclude) == 0 && spec.MaxSizeBytes == 0 {
		return nil
	}
	matcher := newMatcher(spec)
	return func(path string, meta string) bool {
		if matcher == nil {
			return true
		}
		size := metaSize(meta)
		if size > int64(int(^uint(0)>>1)) {
			size = int64(int(^uint(0) >> 1))
		}
		if matcher.IsExcluded(path, int(size)) {
			return false
		}
		return true
	}
}

func metaSize(metaStr string) int64 {
	if metaStr == "" {
		return 0
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
		return 0
	}
	if v, ok := meta["size"]; ok {
		return toInt64(v)
	}
	if v, ok := meta["bytes"]; ok {
		return toInt64(v)
	}
	return 0
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	case float32:
		return int64(t)
	default:
		return 0
	}
}

func splitFile(relPath string, data []byte, factory *splitter.Factory) ([]schema.Document, error) {
	s := factory.GetSplitter(relPath, len(data))
	content := data
	var fragments []*document.Fragment
	if cs, ok := s.(splitter.ContentSplitter); ok {
		fragments, content = cs.SplitWithContent(data, map[string]interface{}{
			meta.DocumentID: relPath,
			meta.PathKey:    relPath,
		})
	} else {
		fragments = s.Split(data, map[string]interface{}{
			meta.DocumentID: relPath,
			meta.PathKey:    relPath,
		})
	}
	if content == nil {
		content = data
	}
	docs := make([]schema.Document, 0, len(fragments))
	for _, frag := range fragments {
		docs = append(docs, frag.NewDocument(relPath, content))
	}
	return docs, nil
}

func ensureSchema(ctx context.Context, q sqlQueryer, driver string) error {
	dialect := resolveDialect(driver)
	if dialect != dialectSQLite {
		return nil
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS vec_dataset (
			dataset_id TEXT PRIMARY KEY,
			description TEXT,
			source_uri TEXT,
			last_scn INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS vec_dataset_scn (
			dataset_id TEXT PRIMARY KEY,
			next_scn INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS vec_shadow_log (
			dataset_id TEXT NOT NULL,
			shadow_table TEXT NOT NULL,
			scn INTEGER NOT NULL,
			op TEXT NOT NULL,
			document_id TEXT NOT NULL,
			payload TEXT,
			PRIMARY KEY(dataset_id, shadow_table, scn)
		);`,
		`CREATE TABLE IF NOT EXISTS vec_sync_state (
			dataset_id TEXT NOT NULL,
			shadow_table TEXT NOT NULL,
			last_scn INTEGER NOT NULL DEFAULT 0,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(dataset_id, shadow_table)
		);`,
		`CREATE TABLE IF NOT EXISTS emb_root (
			dataset_id TEXT PRIMARY KEY,
			source_uri TEXT,
			description TEXT,
			last_indexed_at DATETIME,
			last_scn INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS emb_root_config (
			dataset_id TEXT PRIMARY KEY,
			include_globs TEXT,
			exclude_globs TEXT,
			max_size_bytes INTEGER NOT NULL DEFAULT 0,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS emb_asset (
			dataset_id TEXT NOT NULL,
			asset_id TEXT NOT NULL,
			path TEXT NOT NULL,
			md5 TEXT NOT NULL,
			size INTEGER NOT NULL,
			mod_time DATETIME NOT NULL,
			scn INTEGER NOT NULL,
			archived INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(dataset_id, asset_id)
		);`,
		`CREATE TABLE IF NOT EXISTS _vec_emb_docs (
			dataset_id TEXT NOT NULL,
			id TEXT NOT NULL,
			asset_id TEXT NOT NULL,
			content TEXT,
			meta TEXT,
			embedding BLOB,
			embedding_model TEXT,
			scn INTEGER NOT NULL,
			archived INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(dataset_id, id)
		);`,
		`CREATE TABLE IF NOT EXISTS vector_storage (
			shadow_table_name TEXT NOT NULL,
			dataset_id TEXT NOT NULL DEFAULT '',
			"index" BLOB,
			PRIMARY KEY (shadow_table_name, dataset_id)
		);`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS emb_docs USING vec(doc_id);`,
	}
	for _, stmt := range stmts {
		if _, err := q.ExecContext(ctx, stmt); err != nil {
			if strings.Contains(err.Error(), "no such module: vec") && strings.Contains(stmt, "VIRTUAL TABLE") {
				continue
			}
			return err
		}
	}
	return nil
}

func ensureDataset(ctx context.Context, q sqlQueryer, datasetID, sourcePath, driver string) error {
	dialect := resolveDialect(driver)
	switch dialect {
	case dialectMySQL:
		if _, err := q.ExecContext(ctx, `INSERT IGNORE INTO vec_dataset(dataset_id, description, source_uri, last_scn) VALUES(?,?,?,0)`, datasetID, datasetID, sourcePath); err != nil {
			return err
		}
		_, err := q.ExecContext(ctx, `INSERT IGNORE INTO emb_root(dataset_id, source_uri, description, last_indexed_at, last_scn) VALUES(?,?,?,?,0)`, datasetID, sourcePath, datasetID, time.Now())
		return err
	default:
		if _, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO vec_dataset(dataset_id, description, source_uri, last_scn) VALUES(?,?,?,0)`, datasetID, datasetID, sourcePath); err != nil {
			return err
		}
		_, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO emb_root(dataset_id, source_uri, description, last_indexed_at, last_scn) VALUES(?,?,?,?,0)`, datasetID, sourcePath, datasetID, time.Now())
		return err
	}
}

func upsertRootConfig(ctx context.Context, q sqlQueryer, datasetID, includeGlobs, excludeGlobs string, maxSizeBytes int64, driver string) error {
	if datasetID == "" {
		return nil
	}
	dialect := resolveDialect(driver)
	switch dialect {
	case dialectMySQL:
		_, err := q.ExecContext(ctx, `INSERT INTO emb_root_config(dataset_id, include_globs, exclude_globs, max_size_bytes, updated_at)
VALUES(?,?,?,?,CURRENT_TIMESTAMP)
ON DUPLICATE KEY UPDATE
include_globs=VALUES(include_globs),
exclude_globs=VALUES(exclude_globs),
max_size_bytes=VALUES(max_size_bytes),
updated_at=CURRENT_TIMESTAMP`, datasetID, includeGlobs, excludeGlobs, maxSizeBytes)
		return err
	default:
		_, err := q.ExecContext(ctx, `INSERT INTO emb_root_config(dataset_id, include_globs, exclude_globs, max_size_bytes, updated_at)
VALUES(?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(dataset_id) DO UPDATE SET
include_globs=excluded.include_globs,
exclude_globs=excluded.exclude_globs,
max_size_bytes=excluded.max_size_bytes,
updated_at=CURRENT_TIMESTAMP`, datasetID, includeGlobs, excludeGlobs, maxSizeBytes)
		return err
	}
}

func encodeGlobList(globs []string) string {
	if len(globs) == 0 {
		return ""
	}
	b, err := json.Marshal(globs)
	if err != nil {
		return ""
	}
	return string(b)
}

func loadAssets(ctx context.Context, q sqlQueryer, datasetID string) (map[string]assetInfo, error) {
	rows, err := q.QueryContext(ctx, `SELECT asset_id, md5, archived FROM emb_asset WHERE dataset_id = ?`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assets := map[string]assetInfo{}
	for rows.Next() {
		var id string
		var md5hex string
		var archived int
		if err := rows.Scan(&id, &md5hex, &archived); err != nil {
			return nil, err
		}
		assets[id] = assetInfo{md5: md5hex, archived: archived != 0}
	}
	return assets, rows.Err()
}

func nextSCN(ctx context.Context, q sqlQueryer, datasetID, driver string) (uint64, error) {
	dialect := resolveDialect(driver)
	switch dialect {
	case dialectMySQL:
		if _, err := q.ExecContext(ctx, `INSERT IGNORE INTO vec_dataset_scn(dataset_id, next_scn) VALUES(?, 0)`, datasetID); err != nil {
			return 0, err
		}
	default:
		if _, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO vec_dataset_scn(dataset_id, next_scn) VALUES(?, 0)`, datasetID); err != nil {
			return 0, err
		}
	}
	if _, err := q.ExecContext(ctx, `UPDATE vec_dataset_scn SET next_scn = next_scn + 1 WHERE dataset_id = ?`, datasetID); err != nil {
		return 0, err
	}
	var scn uint64
	if err := q.QueryRowContext(ctx, `SELECT next_scn FROM vec_dataset_scn WHERE dataset_id = ?`, datasetID).Scan(&scn); err != nil {
		return 0, err
	}
	if _, err := q.ExecContext(ctx, `UPDATE vec_dataset SET last_scn = CASE WHEN last_scn < ? THEN ? ELSE last_scn END WHERE dataset_id = ?`, scn, scn, datasetID); err != nil {
		return 0, err
	}
	if _, err := q.ExecContext(ctx, `UPDATE emb_root SET last_scn = CASE WHEN last_scn < ? THEN ? ELSE last_scn END, last_indexed_at = CURRENT_TIMESTAMP WHERE dataset_id = ?`, scn, scn, datasetID); err != nil {
		return 0, err
	}
	return scn, nil
}

func extractMetaPath(metaStr string) string {
	if metaStr == "" {
		return ""
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
		return ""
	}
	if v, ok := meta["path"].(string); ok {
		return v
	}
	return ""
}

func upsertAsset(ctx context.Context, q sqlQueryer, datasetID string, f fileItem, scn uint64, driver string) error {
	dialect := resolveDialect(driver)
	switch dialect {
	case dialectMySQL:
		_, err := q.ExecContext(ctx, `INSERT INTO emb_asset(dataset_id, asset_id, path, md5, size, mod_time, scn, archived)
VALUES(?,?,?,?,?,?,?,0)
ON DUPLICATE KEY UPDATE
	path=VALUES(path),
	md5=VALUES(md5),
	size=VALUES(size),
	mod_time=VALUES(mod_time),
	scn=VALUES(scn),
	archived=0`,
			datasetID, f.assetID, f.rel, f.md5, f.size, f.modTime, scn,
		)
		return err
	default:
		_, err := q.ExecContext(ctx, `INSERT INTO emb_asset(dataset_id, asset_id, path, md5, size, mod_time, scn, archived)
VALUES(?,?,?,?,?,?,?,0)
ON CONFLICT(dataset_id, asset_id) DO UPDATE SET
	path=excluded.path,
	md5=excluded.md5,
	size=excluded.size,
	mod_time=excluded.mod_time,
	scn=excluded.scn,
	archived=0`,
			datasetID, f.assetID, f.rel, f.md5, f.size, f.modTime, scn,
		)
		return err
	}
}

func upsertDocuments(ctx context.Context, q sqlQueryer, datasetID, assetID string, docs []schema.Document, emb embeddings.Embedder, batchSize int, scn uint64, md5hex, relPath, model, driver string) (int, error) {
	docsTable := localDocsTable(driver)
	if _, err := q.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE dataset_id = ? AND asset_id = ?`, docsTable), datasetID, assetID); err != nil {
		return 0, err
	}
	if len(docs) == 0 {
		return 0, nil
	}
	vectors, tokens, err := embedDocuments(ctx, emb, docs, batchSize)
	if err != nil {
		return 0, err
	}
	var stmtSQL string
	switch resolveDialect(driver) {
	case dialectMySQL:
		stmtSQL = fmt.Sprintf(`INSERT INTO %s(dataset_id, id, asset_id, content, meta, embedding, embedding_model, scn, archived)
VALUES(?,?,?,?,?,?,?,?,0)
ON DUPLICATE KEY UPDATE
	asset_id=VALUES(asset_id),
	content=VALUES(content),
	meta=VALUES(meta),
	embedding=VALUES(embedding),
	embedding_model=VALUES(embedding_model),
	scn=VALUES(scn),
	archived=0`, docsTable)
	default:
		stmtSQL = fmt.Sprintf(`INSERT INTO %s(dataset_id, id, asset_id, content, meta, embedding, embedding_model, scn, archived)
VALUES(?,?,?,?,?,?,?,?,0)
ON CONFLICT(dataset_id, id) DO UPDATE SET
	asset_id=excluded.asset_id,
	content=excluded.content,
	meta=excluded.meta,
	embedding=excluded.embedding,
	embedding_model=excluded.embedding_model,
	scn=excluded.scn,
	archived=0`, docsTable)
	}
	stmt, err := q.PrepareContext(ctx, stmtSQL)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for i, doc := range docs {
		fragID := fragmentID(doc, relPath)
		metaJSON, err := decorateMeta(doc.Metadata, datasetID, assetID, relPath, md5hex)
		if err != nil {
			return 0, err
		}
		blob, err := vector.EncodeEmbedding(vectors[i])
		if err != nil {
			return 0, err
		}
		if _, err := stmt.ExecContext(ctx, datasetID, fragID, assetID, doc.PageContent, metaJSON, blob, model, scn); err != nil {
			return 0, err
		}
	}
	return tokens, nil
}

type usageEmbedder interface {
	EmbedDocumentsWithUsage(ctx context.Context, docs []string) ([][]float32, int, error)
}

func embedDocuments(ctx context.Context, emb embeddings.Embedder, docs []schema.Document, batchSize int) ([][]float32, int, error) {
	if batchSize <= 0 {
		batchSize = 64
	}
	out := make([][]float32, 0, len(docs))
	totalTokens := 0
	for i := 0; i < len(docs); i += batchSize {
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		batch := docs[i:end]
		texts := make([]string, len(batch))
		for j := range batch {
			texts[j] = batch[j].PageContent
		}
		var (
			vecs   [][]float32
			tokens int
			err    error
		)
		if ue, ok := emb.(usageEmbedder); ok {
			vecs, tokens, err = ue.EmbedDocumentsWithUsage(ctx, texts)
			totalTokens += tokens
		} else {
			vecs, err = emb.EmbedDocuments(ctx, texts)
		}
		if err != nil {
			return nil, 0, err
		}
		if len(vecs) != len(texts) {
			return nil, 0, fmt.Errorf("embedder returned %d vectors for %d docs", len(vecs), len(texts))
		}
		out = append(out, vecs...)
	}
	return out, totalTokens, nil
}

func fragmentID(doc schema.Document, relPath string) string {
	if v, ok := doc.Metadata[meta.FragmentID]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	start := 0
	end := 0
	if v, ok := doc.Metadata["start"].(int); ok {
		start = v
	} else if v, ok := doc.Metadata["start"].(float64); ok {
		start = int(v)
	}
	if v, ok := doc.Metadata["end"].(int); ok {
		end = v
	} else if v, ok := doc.Metadata["end"].(float64); ok {
		end = int(v)
	}
	frag := &document.Fragment{Start: start, End: end}
	return frag.ID(relPath)
}

func decorateMeta(metaIn map[string]interface{}, datasetID, assetID, relPath, md5hex string) (string, error) {
	metaOut := map[string]interface{}{}
	for k, v := range metaIn {
		metaOut[k] = v
	}
	metaOut["dataset_id"] = datasetID
	metaOut["asset_id"] = assetID
	metaOut["path"] = relPath
	metaOut["md5"] = md5hex
	data, err := json.Marshal(metaOut)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func archiveAsset(ctx context.Context, q sqlQueryer, datasetID, assetID string, scn uint64, driver string) error {
	if _, err := q.ExecContext(ctx, `UPDATE emb_asset SET archived=1, scn=? WHERE dataset_id=? AND asset_id=?`, scn, datasetID, assetID); err != nil {
		return err
	}
	docsTable := localDocsTable(driver)
	_, err := q.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET archived=1, scn=? WHERE dataset_id=? AND asset_id=?`, docsTable), scn, datasetID, assetID)
	return err
}

func pruneArchived(ctx context.Context, q sqlQueryer, datasetID, driver string) error {
	docsTable := localDocsTable(driver)
	if _, err := q.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE dataset_id=? AND archived=1`, docsTable), datasetID); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `DELETE FROM emb_asset WHERE dataset_id=? AND archived=1`, datasetID)
	return err
}

func pruneMaxSCN(ctx context.Context, q sqlQueryer, datasetID, syncShadow string) (int64, error) {
	var last int64
	err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(last_scn), 0) FROM vec_sync_state WHERE dataset_id = ? AND shadow_table = ?`, datasetID, syncShadow).Scan(&last)
	if err != nil {
		if isMissingTableErr(err, "vec_sync_state") {
			return 0, nil
		}
		return 0, err
	}
	return last, nil
}

func pruneArchivedBefore(ctx context.Context, q sqlQueryer, datasetID string, scn int64, driver string) error {
	if scn <= 0 {
		return nil
	}
	docsTable := localDocsTable(driver)
	if _, err := q.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE dataset_id = ? AND archived = 1 AND scn <= ?`, docsTable), datasetID, scn); err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM emb_asset WHERE dataset_id = ? AND archived = 1 AND scn <= ?`, datasetID, scn); err != nil {
		return err
	}
	return nil
}

func checkIntegrity(ctx context.Context, q sqlQueryer, datasetID, driver string) (*IntegrityStats, error) {
	var stats IntegrityStats
	stats.DatasetID = datasetID
	docsTable := localDocsTable(driver)
	if err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*), SUM(CASE WHEN archived=1 THEN 1 ELSE 0 END), SUM(CASE WHEN archived=0 THEN 1 ELSE 0 END)
FROM %s WHERE dataset_id = ?`, docsTable), datasetID).Scan(&stats.Docs, &stats.DocsArchived, &stats.DocsActive); err != nil {
		return nil, err
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*), SUM(CASE WHEN archived=1 THEN 1 ELSE 0 END), SUM(CASE WHEN archived=0 THEN 1 ELSE 0 END)
FROM emb_asset WHERE dataset_id = ?`, datasetID).Scan(&stats.Assets, &stats.AssetsArchived, &stats.AssetsActive); err != nil {
		return nil, err
	}
	if err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s d LEFT JOIN emb_asset a
ON a.dataset_id = d.dataset_id AND a.asset_id = d.asset_id
WHERE d.dataset_id = ? AND a.asset_id IS NULL`, docsTable), datasetID).Scan(&stats.OrphanDocs); err != nil {
		return nil, err
	}
	if err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM emb_asset a LEFT JOIN %s d
ON d.dataset_id = a.dataset_id AND d.asset_id = a.asset_id
WHERE a.dataset_id = ? AND d.id IS NULL`, docsTable), datasetID).Scan(&stats.OrphanAssets); err != nil {
		return nil, err
	}
	if err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE dataset_id = ? AND embedding IS NULL`, docsTable), datasetID).Scan(&stats.MissingEmbeddings); err != nil {
		return nil, err
	}
	return &stats, nil
}

func fallbackSearch(ctx context.Context, q sqlQueryer, dataset string, qvec []float32, minScore float64, limit int) ([]SearchResult, error) {
	rows, err := q.QueryContext(ctx, `SELECT id, content, meta, embedding FROM _vec_emb_docs WHERE dataset_id = ? AND archived = 0`, dataset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type hit struct {
		id      string
		score   float32
		content string
		meta    string
	}
	var hits []hit
	for rows.Next() {
		var id, content, metaJSON string
		var emb []byte
		if err := rows.Scan(&id, &content, &metaJSON, &emb); err != nil {
			return nil, err
		}
		vecs, err := vector.DecodeEmbedding(emb)
		if err != nil {
			continue
		}
		sim := cosine(qvec, vecs)
		if float64(sim) < minScore {
			continue
		}
		hits = append(hits, hit{id: id, score: sim, content: content, meta: metaJSON})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	out := make([]SearchResult, 0, len(hits))
	for _, h := range hits {
		out = append(out, SearchResult{
			ID:      h.id,
			Score:   float64(h.score),
			Content: h.content,
			Meta:    h.meta,
			Path:    extractMetaPath(h.meta),
		})
	}
	return out, nil
}

func isMissingTableErr(err error, table string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "no such table") && strings.Contains(msg, strings.ToLower(table)) {
		return true
	}
	// MySQL: Error 1146: Table 'db.table' doesn't exist
	if strings.Contains(msg, "error 1146") && strings.Contains(msg, "doesn't exist") && strings.Contains(msg, strings.ToLower(table)) {
		return true
	}
	return false
}

func cosine(a, b []float32) float32 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var dot, na, nb float32
	for i := 0; i < n; i++ {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / float32((sqrt(float64(na)) * sqrt(float64(nb))))
}

func sqrt(x float64) float64 {
	if x == 0 {
		return 0
	}
	z := x
	for i := 0; i < 10; i++ {
		z -= (z*z - x) / (2 * z)
	}
	return z
}
