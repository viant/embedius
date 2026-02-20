package sqlitevec

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/viant/embedius/db/sqliteutil"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/indexer/fs"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectordb"
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
	walEnabled    bool
	busyTimeoutMS int
	syncMu        sync.Mutex
	syncLast      map[string]time.Time
	syncInFlight  map[string]bool
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

// WithWAL enables WAL journal mode for shared access.
func WithWAL(enabled bool) Option {
	return func(s *Store) { s.walEnabled = enabled }
}

// WithBusyTimeout sets the SQLite busy_timeout in milliseconds.
func WithBusyTimeout(ms int) Option {
	return func(s *Store) { s.busyTimeoutMS = ms }
}

// NewStore opens/initializes a sqlite-vec Store.
func NewStore(opts ...Option) (*Store, error) {
	s := &Store{
		vtable:        "emb_docs",
		ensureSchema:  true,
		embedBatch:    64,
		walEnabled:    true,
		busyTimeoutMS: 5000,
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
		db, err := engine.Open(sqliteutil.EnsurePragmas(s.dsn, s.walEnabled, s.busyTimeoutMS))
		if err != nil {
			return nil, err
		}
		s.db = db
		s.openedLocally = true
	}
	if err := vec.Register(s.db); err != nil {
		return nil, err
	}
	if s.walEnabled {
		if _, err := s.db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
			return nil, fmt.Errorf("sqlitevec: set WAL mode: %w", err)
		}
	}
	if s.busyTimeoutMS > 0 {
		if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA busy_timeout=%d`, s.busyTimeoutMS)); err != nil {
			return nil, fmt.Errorf("sqlitevec: set busy_timeout: %w", err)
		}
	}
	checkMatchSupport(s.db)
	if s.ensureSchema {
		if err := s.ensureSchemaDDL(context.Background()); err != nil {
			return nil, err
		}
		if err := s.verifySchema(context.Background()); err != nil {
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

// ExistingAssetMD5s returns md5 hashes that already exist for the dataset.
func (s *Store) ExistingAssetMD5s(ctx context.Context, dataset string, md5s []string) (map[string]bool, error) {
	out := map[string]bool{}
	if s == nil || s.db == nil || len(md5s) == 0 {
		return out, nil
	}
	dedup := make(map[string]struct{}, len(md5s))
	uniq := make([]string, 0, len(md5s))
	for _, md5hex := range md5s {
		md5hex = strings.TrimSpace(md5hex)
		if md5hex == "" {
			continue
		}
		if _, ok := dedup[md5hex]; ok {
			continue
		}
		dedup[md5hex] = struct{}{}
		uniq = append(uniq, md5hex)
	}
	if len(uniq) == 0 {
		return out, nil
	}
	const batch = 500
	for i := 0; i < len(uniq); i += batch {
		end := i + batch
		if end > len(uniq) {
			end = len(uniq)
		}
		args := make([]any, 0, end-i+1)
		args = append(args, dataset)
		placeholders := make([]string, 0, end-i)
		for _, md5hex := range uniq[i:end] {
			args = append(args, md5hex)
			placeholders = append(placeholders, "?")
		}
		query := fmt.Sprintf(`SELECT md5 FROM emb_asset WHERE dataset_id = ? AND archived = 0 AND md5 IN (%s)`, strings.Join(placeholders, ","))
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var md5hex string
			if err := rows.Scan(&md5hex); err != nil {
				_ = rows.Close()
				return out, err
			}
			out[md5hex] = true
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return out, err
		}
		_ = rows.Close()
	}
	return out, nil
}

// AssetMD5s returns all md5 hashes for non-archived assets in the dataset.
func (s *Store) AssetMD5s(ctx context.Context, dataset string) (map[string]bool, error) {
	out := map[string]bool{}
	if s == nil || s.db == nil {
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT md5 FROM emb_asset WHERE dataset_id = ? AND archived = 0`, dataset)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var md5hex string
		if err := rows.Scan(&md5hex); err != nil {
			return out, err
		}
		md5hex = strings.TrimSpace(md5hex)
		if md5hex != "" {
			out[md5hex] = true
		}
	}
	return out, rows.Err()
}

// AssetMetaByPath returns asset metadata keyed by path for non-archived assets.
func (s *Store) AssetMetaByPath(ctx context.Context, dataset string) (map[string]fs.AssetMeta, error) {
	if s == nil || s.db == nil || strings.TrimSpace(dataset) == "" {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT path, size, md5 FROM emb_asset WHERE dataset_id = ? AND archived = 0`, dataset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]fs.AssetMeta{}
	for rows.Next() {
		var path string
		var size int64
		var md5hex string
		if err := rows.Scan(&path, &size, &md5hex); err != nil {
			return nil, err
		}
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		out[path] = fs.AssetMeta{Size: size, MD5: strings.TrimSpace(md5hex)}
	}
	return out, rows.Err()
}

// SyncUpstream synchronizes upstream changes into the local sqlitevec store.
func (s *Store) SyncUpstream(ctx context.Context, cfg vectordb.UpstreamSyncConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlitevec: store not initialized")
	}
	if cfg.UpstreamDB == nil {
		return fmt.Errorf("sqlitevec: upstream db is required")
	}
	dataset := strings.TrimSpace(cfg.DatasetID)
	if dataset == "" {
		dataset = defaultNamespace
	}
	shadow := strings.TrimSpace(cfg.Shadow)
	if shadow == "" {
		shadow = "shadow_vec_docs"
	}
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = 200
	}
	localShadow := strings.TrimSpace(cfg.LocalShadow)
	if localShadow == "" {
		localShadow = s.shadow
	}
	assetTable := strings.TrimSpace(cfg.AssetTable)
	if assetTable == "" {
		assetTable = "emb_asset"
	}
	if cfg.Logf != nil {
		cfg.Logf("embedius upstream sync start dataset=%q shadow=%q", dataset, shadow)
	}
	shouldRun, background := s.shouldRunSync(dataset, cfg.MinInterval, cfg.Background)
	if !shouldRun {
		return nil
	}
	runCtx := ctx
	if background {
		runCtx = context.Background()
	}
	run := func(runCtx context.Context) error {
		err := SyncUpstream(runCtx, s.db, cfg.UpstreamDB, SyncConfig{
			DatasetID:      dataset,
			UpstreamShadow: shadow,
			LocalShadow:    localShadow,
			AssetTable:     assetTable,
			BatchSize:      batch,
			ForceSync:      cfg.Force,
			Logf:           cfg.Logf,
			Filter:         cfg.Filter,
		})
		s.markSyncComplete(dataset)
		return err
	}
	if background {
		go func() {
			if err := run(runCtx); err != nil && cfg.Logf != nil {
				cfg.Logf("embedius upstream sync failed dataset=%q err=%v", dataset, err)
			} else if err == nil && cfg.Logf != nil {
				cfg.Logf("embedius upstream sync done dataset=%q shadow=%q", dataset, shadow)
			}
		}()
		return nil
	}
	err := run(runCtx)
	if err != nil {
		return err
	}
	if cfg.Logf != nil {
		cfg.Logf("embedius upstream sync done dataset=%q shadow=%q", dataset, shadow)
	}
	return nil
}

func (s *Store) shouldRunSync(dataset string, minInterval time.Duration, background bool) (bool, bool) {
	if dataset == "" {
		return false, false
	}
	if minInterval <= 0 {
		minInterval = time.Hour
	}
	empty := s.datasetEmpty(context.Background(), dataset)
	if empty {
		background = false
	}
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if s.syncInFlight == nil {
		s.syncInFlight = map[string]bool{}
	}
	if s.syncLast == nil {
		s.syncLast = map[string]time.Time{}
	}
	if s.syncInFlight[dataset] {
		return false, false
	}
	if last, ok := s.syncLast[dataset]; ok && !last.IsZero() && time.Since(last) < minInterval {
		return false, false
	}
	s.syncInFlight[dataset] = true
	return true, background
}

func (s *Store) markSyncComplete(dataset string) {
	if dataset == "" {
		return
	}
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if s.syncInFlight != nil {
		delete(s.syncInFlight, dataset)
	}
	if s.syncLast == nil {
		s.syncLast = map[string]time.Time{}
	}
	s.syncLast[dataset] = time.Now()
}

func (s *Store) datasetEmpty(ctx context.Context, dataset string) bool {
	if s == nil || s.db == nil || strings.TrimSpace(dataset) == "" {
		return false
	}
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM emb_asset WHERE dataset_id = ?`, dataset).Scan(&count); err != nil {
		return false
	}
	return count == 0
}

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

	embedCtx, cancel := embeddingContext(ctx)
	defer cancel()
	qvec, err := options.Embedder.EmbedQuery(embedCtx, query)
	if err != nil {
		return nil, err
	}
	blob, err := vector.EncodeEmbedding(qvec)
	if err != nil {
		return nil, err
	}
	return s.queryOnce(ctx, dataset, blob, k, options.Offset)
}

func (s *Store) similaritySearchWindows(ctx context.Context, query string, k int, options vectorstores.Options) ([]schema.Document, error) {
	windows := splitQueryWindows(query, options.MaxQueryBytes, options.QueryOverlap)
	if len(windows) == 0 {
		windows = []string{query}
	}
	embedCtx, cancel := embeddingContext(ctx)
	defer cancel()
	vecs, err := options.Embedder.EmbedDocuments(embedCtx, windows)
	if err != nil {
		qvec, e2 := options.Embedder.EmbedQuery(embedCtx, query)
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
	if options.Offset > 0 {
		candK += options.Offset
	}
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
		docs, err := s.queryOnce(ctx, dataset, blob, candK, 0)
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
	offset := options.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(pairs) {
		return nil, nil
	}
	pairs = pairs[offset:]
	if len(pairs) > k {
		pairs = pairs[:k]
	}
	out := make([]schema.Document, len(pairs))
	for i := range pairs {
		out[i] = pairs[i].doc
	}
	return out, nil
}

func embeddingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.Background(), func() {}
	}
	base := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		const minEmbedTimeout = 15 * time.Second
		if time.Until(deadline) < minEmbedTimeout {
			return context.WithTimeout(base, minEmbedTimeout)
		}
		return context.WithDeadline(base, deadline)
	}
	return base, func() {}
}

func (s *Store) queryOnce(ctx context.Context, dataset string, blob []byte, k int, offset int) ([]schema.Document, error) {
	if offset < 0 {
		offset = 0
	}
	query := fmt.Sprintf(`SELECT d.id, d.content, d.meta, v.match_score
FROM %s v
JOIN %s d ON d.dataset_id = v.dataset_id AND d.id = v.doc_id
WHERE v.dataset_id = ?
  AND v.doc_id MATCH ?
  AND d.archived = 0
ORDER BY v.match_score DESC
LIMIT ? OFFSET ?`, s.vtable, s.shadow)

	rows, err := s.db.QueryContext(ctx, query, dataset, blob, k, offset)
	if err != nil {
		if isMatchUnavailable(err) {
			fmt.Printf("sqlitevec: MATCH unavailable, falling back to brute-force search (performance may degrade): %v\n", err)
			return s.fallbackSearch(ctx, dataset, blob, k, offset)
		}
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

func (s *Store) fallbackSearch(ctx context.Context, dataset string, blob []byte, k int, offset int) ([]schema.Document, error) {
	qvec, err := vector.DecodeEmbedding(blob)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT id, content, meta, embedding FROM %s WHERE dataset_id = ? AND archived = 0`, s.shadow), dataset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type hit struct {
		doc   schema.Document
		score float32
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
		score := cosine(qvec, vecs)
		metaMap, err := decodeMeta(metaJSON)
		if err != nil {
			continue
		}
		if _, ok := metaMap[meta.FragmentID]; !ok {
			metaMap[meta.FragmentID] = id
		}
		hits = append(hits, hit{
			doc:   schema.Document{PageContent: content, Metadata: metaMap, Score: score},
			score: score,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	if offset < 0 {
		offset = 0
	}
	if offset >= len(hits) {
		return nil, nil
	}
	hits = hits[offset:]
	if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	out := make([]schema.Document, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.doc)
	}
	return out, nil
}

func isMatchUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "unable to use function MATCH") ||
		strings.Contains(msg, "no such module: vec") ||
		strings.Contains(msg, "no such table:")
}

var matchCheckOnce sync.Once

func checkMatchSupport(db *sql.DB) {
	matchCheckOnce.Do(func() {
		if db == nil {
			return
		}
		// Assume MATCH is supported; fall back on runtime errors with a warning.
	})
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
	return dot / float32(math.Sqrt(float64(na))*math.Sqrt(float64(nb)))
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
	vtabStmt := fmt.Sprintf(`CREATE VIRTUAL TABLE IF NOT EXISTS %s USING vec(doc_id%s);`, s.vtable, s.dbPathClause(ctx))
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
		`CREATE TABLE IF NOT EXISTS emb_root_config (
			dataset_id      TEXT PRIMARY KEY,
			include_globs   TEXT,
			exclude_globs   TEXT,
			max_size_bytes  INTEGER NOT NULL DEFAULT 0,
			updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
		vtabStmt,
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

func (s *Store) dbPathClause(ctx context.Context) string {
	path := resolveDBPath(ctx, s.db)
	if strings.TrimSpace(path) == "" {
		return ""
	}
	return fmt.Sprintf(", dbpath=%s", sqliteQuote(path))
}

func sqliteQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func resolveDBPath(ctx context.Context, db *sql.DB) string {
	if db == nil {
		return ""
	}
	rows, err := db.QueryContext(ctx, `SELECT name, file FROM pragma_database_list`)
	if err != nil {
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		var name, file string
		if err := rows.Scan(&name, &file); err != nil {
			return ""
		}
		if name == "main" && strings.TrimSpace(file) != "" {
			return file
		}
	}
	return ""
}

func (s *Store) verifySchema(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	names := []string{s.shadow, s.vtable}
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			continue
		}
		var found string
		err := s.db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&found)
		if err == sql.ErrNoRows {
			return fmt.Errorf("sqlitevec: expected table %q not found", name)
		}
		if err != nil {
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
