package sqlitevec

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectordb/meta"
	"github.com/viant/embedius/vectorstores"
	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vec"
	"github.com/viant/sqlite-vec/vector"
)

const defaultNamespace = "default"

// SCNAllocator returns a new SCN for the given dataset.
// When set, AddDocuments/Remove will stamp scn on written rows.
type SCNAllocator func(ctx context.Context, datasetID string) (int64, error)

// Store is a sqlite-vec backed VectorStore.
type Store struct {
	db            *sql.DB
	dsn           string
	vtable        string
	shadow        string
	ensureSchema  bool
	embedBatch    int
	embedModel    string
	scnAllocator  SCNAllocator
	openedLocally bool
}

// Option configures the sqlite-vec store.
type Option func(*Store)

// WithDB sets an existing *sql.DB to use.
func WithDB(db *sql.DB) Option {
	return func(s *Store) { s.db = db }
}

// WithDSN sets the SQLite DSN to open (e.g. /path/to/db.sqlite).
func WithDSN(dsn string) Option {
	return func(s *Store) { s.dsn = dsn }
}

// WithVTable sets the vec virtual table name (default: emb_docs).
func WithVTable(name string) Option {
	return func(s *Store) { s.vtable = name }
}

// WithEnsureSchema controls whether schema and indexes are created automatically.
func WithEnsureSchema(enabled bool) Option {
	return func(s *Store) { s.ensureSchema = enabled }
}

// WithEmbedBatchSize sets the embedding batch size for AddDocuments.
func WithEmbedBatchSize(size int) Option {
	return func(s *Store) { s.embedBatch = size }
}

// WithEmbeddingModel sets the embedding_model stored with rows.
func WithEmbeddingModel(model string) Option {
	return func(s *Store) { s.embedModel = model }
}

// WithSCNAllocator sets the SCN allocator used for writes.
func WithSCNAllocator(fn SCNAllocator) Option {
	return func(s *Store) { s.scnAllocator = fn }
}

// NewStore opens/initializes a sqlite-vec Store.
func NewStore(opts ...Option) (*Store, error) {
	s := &Store{
		vtable:       "emb_docs",
		ensureSchema: true,
		embedBatch:   64,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.vtable == "" {
		s.vtable = "emb_docs"
	}
	s.shadow = "_vec_" + s.vtable

	if s.db == nil {
		if s.dsn == "" {
			return nil, fmt.Errorf("sqlitevec: dsn required")
		}
		db, err := engine.Open(s.dsn)
		if err != nil {
			return nil, err
		}
		s.db = db
		s.db.SetMaxOpenConns(4)
		s.db.SetMaxIdleConns(4)
		s.openedLocally = true
	}
	if err := vec.Register(s.db); err != nil {
		return nil, err
	}
	if s.ensureSchema {
		if err := s.ensureSchemaDDL(context.Background()); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Close closes the underlying DB if Store opened it.
func (s *Store) Close() error {
	if s.openedLocally && s.db != nil {
		return s.db.Close()
	}
	return nil
}

// DB exposes the underlying sql.DB.
func (s *Store) DB() *sql.DB { return s.db }

// SetSCNAllocator overrides the SCN allocator after store creation.
func (s *Store) SetSCNAllocator(fn SCNAllocator) { s.scnAllocator = fn }

// Persist is a no-op for sqlite-vec; data is persisted on each write.
func (s *Store) Persist(ctx context.Context) error { return nil }

// AddDocuments embeds and upserts documents into the shadow table.
func (s *Store) AddDocuments(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) ([]string, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.Embedder == nil {
		return nil, fmt.Errorf("embedder is required")
	}
	dataset := options.NameSpace
	if dataset == "" {
		dataset = defaultNamespace
	}

	scn := int64(0)
	if s.scnAllocator != nil {
		var err error
		scn, err = s.scnAllocator(ctx, dataset)
		if err != nil {
			return nil, err
		}
	}

	vecs, err := embedDocuments(ctx, options.Embedder, docs, s.embedBatch)
	if err != nil {
		return nil, err
	}
	stmt, err := s.db.PrepareContext(ctx, fmt.Sprintf(`INSERT INTO %s(dataset_id, id, asset_id, content, meta, embedding, embedding_model, scn, archived)
VALUES(?,?,?,?,?,?,?,?,0)
ON CONFLICT(dataset_id, id) DO UPDATE SET
	asset_id=excluded.asset_id,
	content=excluded.content,
	meta=excluded.meta,
	embedding=excluded.embedding,
	embedding_model=excluded.embedding_model,
	scn=excluded.scn,
	archived=0`, s.shadow))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	ids := make([]string, len(docs))
	for i, doc := range docs {
		if doc.Metadata == nil {
			doc.Metadata = map[string]interface{}{}
		}
		id := fragmentID(doc)
		ids[i] = id
		metaJSON, assetID, err := encodeMeta(doc.Metadata)
		if err != nil {
			return nil, err
		}
		if assetID == "" {
			assetID = id
		}
		blob, err := vector.EncodeEmbedding(vecs[i])
		if err != nil {
			return nil, err
		}
		model := s.embedModel
		if v, ok := doc.Metadata["embedding_model"].(string); ok && v != "" {
			model = v
		}
		if _, err := stmt.ExecContext(ctx, dataset, id, assetID, doc.PageContent, metaJSON, blob, model, scn); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// SimilaritySearch performs a MATCH query over the sqlite-vec virtual table.
func (s *Store) SimilaritySearch(ctx context.Context, query string, k int, opts ...vectorstores.Option) ([]schema.Document, error) {
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.Embedder == nil {
		return nil, fmt.Errorf("embedder is required")
	}
	dataset := options.NameSpace
	if dataset == "" {
		dataset = defaultNamespace
	}
	if k <= 0 {
		k = 10
	}

	if options.MaxQueryBytes > 0 && len(query) > options.MaxQueryBytes {
		return s.similaritySearchWindows(ctx, query, k, options)
	}

	qvec, err := options.Embedder.EmbedQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	blob, err := vector.EncodeEmbedding(qvec)
	if err != nil {
		return nil, err
	}
	return s.queryOnce(ctx, dataset, blob, k)
}

func (s *Store) similaritySearchWindows(ctx context.Context, query string, k int, options vectorstores.Options) ([]schema.Document, error) {
	windows := splitQueryWindows(query, options.MaxQueryBytes, options.QueryOverlap)
	if len(windows) == 0 {
		windows = []string{query}
	}
	vecs, err := options.Embedder.EmbedDocuments(ctx, windows)
	if err != nil {
		qvec, e2 := options.Embedder.EmbedQuery(ctx, query)
		if e2 != nil {
			return nil, err
		}
		vecs = [][]float32{qvec}
	}

	dataset := options.NameSpace
	if dataset == "" {
		dataset = defaultNamespace
	}
	candK := k
	if candK < 32 {
		candK = k * 2
	}

	type agg struct {
		sum   float32
		count int
		max   float32
		doc   schema.Document
	}
	acc := map[string]*agg{}
	for _, v := range vecs {
		if len(v) == 0 {
			continue
		}
		blob, err := vector.EncodeEmbedding(v)
		if err != nil {
			return nil, err
		}
		docs, err := s.queryOnce(ctx, dataset, blob, candK)
		if err != nil {
			return nil, err
		}
		for _, d := range docs {
			a := acc[d.Metadata[meta.FragmentID].(string)]
			if a == nil {
				a = &agg{max: float32(d.Score), doc: d}
				acc[d.Metadata[meta.FragmentID].(string)] = a
			}
			a.sum += float32(d.Score)
			a.count++
			if float32(d.Score) > a.max {
				a.max = float32(d.Score)
				a.doc = d
			}
		}
	}

	useMean := options.QueryAggregator == "mean"
	type pair struct {
		doc   schema.Document
		score float32
	}
	pairs := make([]pair, 0, len(acc))
	for _, a := range acc {
		sc := a.max
		if useMean && a.count > 0 {
			sc = a.sum / float32(a.count)
		}
		d := a.doc
		d.Score = sc
		pairs = append(pairs, pair{doc: d, score: sc})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].score > pairs[j].score })
	if len(pairs) > k {
		pairs = pairs[:k]
	}
	out := make([]schema.Document, len(pairs))
	for i := range pairs {
		out[i] = pairs[i].doc
	}
	return out, nil
}

func (s *Store) queryOnce(ctx context.Context, dataset string, blob []byte, k int) ([]schema.Document, error) {
	query := fmt.Sprintf(`SELECT d.id, d.content, d.meta, v.match_score
FROM %s v
JOIN %s d ON d.dataset_id = v.dataset_id AND d.id = v.doc_id
WHERE v.dataset_id = ?
  AND v.doc_id MATCH ?
  AND d.archived = 0
ORDER BY v.match_score DESC
LIMIT ?`, s.vtable, s.shadow)

	rows, err := s.db.QueryContext(ctx, query, dataset, blob, k)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []schema.Document
	for rows.Next() {
		var id string
		var content string
		var metaJSON string
		var score float64
		if err := rows.Scan(&id, &content, &metaJSON, &score); err != nil {
			return nil, err
		}
		metaMap, err := decodeMeta(metaJSON)
		if err != nil {
			return nil, err
		}
		if _, ok := metaMap[meta.FragmentID]; !ok {
			metaMap[meta.FragmentID] = id
		}
		docs = append(docs, schema.Document{PageContent: content, Metadata: metaMap, Score: float32(score)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return docs, nil
}

// Remove soft-deletes a document by id.
func (s *Store) Remove(ctx context.Context, id string, opts ...vectorstores.Option) error {
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	dataset := options.NameSpace
	if dataset == "" {
		dataset = defaultNamespace
	}
	scn := int64(0)
	if s.scnAllocator != nil {
		var err error
		scn, err = s.scnAllocator(ctx, dataset)
		if err != nil {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET archived=1, scn=? WHERE dataset_id=? AND id=?`, s.shadow), scn, dataset, id)
	return err
}

func (s *Store) ensureSchemaDDL(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS vec_dataset (
			dataset_id   TEXT PRIMARY KEY,
			description  TEXT,
			source_uri   TEXT,
			last_scn     INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS vec_dataset_scn (
			dataset_id TEXT PRIMARY KEY,
			next_scn   INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS vec_shadow_log (
			dataset_id   TEXT NOT NULL,
			shadow_table TEXT NOT NULL,
			scn          INTEGER NOT NULL,
			op           TEXT NOT NULL,
			document_id  TEXT NOT NULL,
			payload      BLOB NOT NULL,
			created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(dataset_id, shadow_table, scn)
		);`,
		`CREATE TABLE IF NOT EXISTS vec_sync_state (
			dataset_id   TEXT NOT NULL,
			shadow_table TEXT NOT NULL,
			last_scn     INTEGER NOT NULL DEFAULT 0,
			updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(dataset_id, shadow_table)
		);`,
		`CREATE TABLE IF NOT EXISTS vector_storage (
			shadow_table_name TEXT NOT NULL,
			dataset_id        TEXT NOT NULL DEFAULT '',
			"index"           BLOB,
			PRIMARY KEY (shadow_table_name, dataset_id)
		);`,
		`CREATE TABLE IF NOT EXISTS emb_root (
			dataset_id      TEXT PRIMARY KEY,
			source_uri      TEXT,
			description     TEXT,
			last_indexed_at TIMESTAMP,
			last_scn        INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS emb_asset (
			dataset_id TEXT NOT NULL,
			asset_id   TEXT NOT NULL,
			path       TEXT NOT NULL,
			md5        TEXT NOT NULL,
			size       INTEGER NOT NULL,
			mod_time   TIMESTAMP NOT NULL,
			scn        INTEGER NOT NULL,
			archived   INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (dataset_id, asset_id)
		);`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			dataset_id       TEXT NOT NULL,
			id               TEXT NOT NULL,
			asset_id         TEXT NOT NULL,
			content          TEXT,
			meta             TEXT,
			embedding        BLOB,
			embedding_model  TEXT,
			scn              INTEGER NOT NULL,
			archived         INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (dataset_id, id)
		);`, s.shadow),
		fmt.Sprintf(`CREATE VIRTUAL TABLE IF NOT EXISTS %s USING vec(doc_id);`, s.vtable),
		`CREATE INDEX IF NOT EXISTS idx_emb_asset_path ON emb_asset(dataset_id, path);`,
		`CREATE INDEX IF NOT EXISTS idx_emb_asset_mod ON emb_asset(dataset_id, mod_time);`,
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_asset ON %s(dataset_id, asset_id);`, s.vtable, s.shadow),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_scn ON %s(dataset_id, scn);`, s.vtable, s.shadow),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_archived ON %s(dataset_id, archived);`, s.vtable, s.shadow),
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func embedDocuments(ctx context.Context, emb embeddings.Embedder, docs []schema.Document, batchSize int) ([][]float32, error) {
	if batchSize <= 0 {
		batchSize = 64
	}
	out := make([][]float32, 0, len(docs))
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
		vecs, err := emb.EmbedDocuments(ctx, texts)
		if err != nil {
			return nil, err
		}
		if len(vecs) != len(texts) {
			return nil, fmt.Errorf("embedder returned %d vectors for %d docs", len(vecs), len(texts))
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func encodeMeta(metaIn map[string]interface{}) (string, string, error) {
	if metaIn == nil {
		metaIn = map[string]interface{}{}
	}
	assetID := ""
	if v, ok := metaIn["asset_id"].(string); ok {
		assetID = v
	}
	data, err := json.Marshal(metaIn)
	if err != nil {
		return "", assetID, err
	}
	return string(data), assetID, nil
}

func decodeMeta(metaJSON string) (map[string]interface{}, error) {
	if metaJSON == "" {
		return map[string]interface{}{}, nil
	}
	metaMap := map[string]interface{}{}
	if err := json.Unmarshal([]byte(metaJSON), &metaMap); err != nil {
		return nil, err
	}
	return metaMap, nil
}

func fragmentID(doc schema.Document) string {
	if doc.Metadata == nil {
		doc.Metadata = map[string]interface{}{}
	}
	if v, ok := doc.Metadata[meta.FragmentID]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	if v, ok := doc.Metadata[meta.DocumentID]; ok {
		if s, ok := v.(string); ok && s != "" {
			start, end := fragmentBounds(doc.Metadata)
			return fmt.Sprintf("%s:%d-%d", s, start, end)
		}
	}
	start, end := fragmentBounds(doc.Metadata)
	return fmt.Sprintf("doc:%d-%d", start, end)
}

func fragmentBounds(metaIn map[string]interface{}) (int, int) {
	start := toInt(metaIn["start"])
	end := toInt(metaIn["end"])
	if end < start {
		end = start
	}
	return start, end
}

func toInt(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case float32:
		return int(t)
	default:
		return 0
	}
}

// splitQueryWindows splits s into UTF-8 safe overlapping windows.
// maxBytes defines window size; overlap defines byte overlap between consecutive windows.
func splitQueryWindows(s string, maxBytes, overlap int) []string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return []string{s}
	}
	if overlap < 0 {
		overlap = 0
	}
	if overlap >= maxBytes {
		overlap = maxBytes / 4
	}
	step := maxBytes - overlap
	if step <= 0 {
		step = maxBytes
	}
	b := []byte(s)
	var out []string
	for start := 0; start < len(b); start += step {
		for start < len(b) && !utf8.RuneStart(b[start]) {
			start++
		}
		if start >= len(b) {
			break
		}
		end := start + maxBytes
		if end >= len(b) {
			end = len(b) - 1
		}
		for end > start && !utf8.RuneStart(b[end]) {
			end--
		}
		if end <= start {
			break
		}
		out = append(out, string(b[start:end]))
		if end == len(b) {
			break
		}
	}
	if len(out) == 0 {
		return []string{s}
	}
	return out
}

// DefaultSCNAllocator returns a simple allocator over vec_dataset_scn in the local DB.
func DefaultSCNAllocator(db *sql.DB) SCNAllocator {
	return func(ctx context.Context, datasetID string) (int64, error) {
		if datasetID == "" {
			return 0, fmt.Errorf("dataset_id is required")
		}
		if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO vec_dataset_scn(dataset_id, next_scn) VALUES(?, 0)`, datasetID); err != nil {
			return 0, err
		}
		if _, err := db.ExecContext(ctx, `UPDATE vec_dataset_scn SET next_scn = next_scn + 1 WHERE dataset_id = ?`, datasetID); err != nil {
			return 0, err
		}
		var scn int64
		if err := db.QueryRowContext(ctx, `SELECT next_scn FROM vec_dataset_scn WHERE dataset_id = ?`, datasetID).Scan(&scn); err != nil {
			return 0, err
		}
		if _, err := db.ExecContext(ctx, `UPDATE vec_dataset SET last_scn = ? WHERE dataset_id = ?`, scn, datasetID); err != nil {
			return 0, err
		}
		if _, err := db.ExecContext(ctx, `UPDATE emb_root SET last_scn = ?, last_indexed_at = CURRENT_TIMESTAMP WHERE dataset_id = ?`, scn, datasetID); err != nil {
			return 0, err
		}
		return scn, nil
	}
}
