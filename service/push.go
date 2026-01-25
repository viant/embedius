package service

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
)

const pushShadowPrefix = "push:"

type pushPayload struct {
	DatasetID      string `json:"dataset_id"`
	ID             string `json:"id"`
	Content        string `json:"content"`
	Meta           string `json:"meta"`
	Embedding      string `json:"embedding"`
	EmbeddingModel string `json:"embedding_model"`
	SCN            int64  `json:"scn"`
	Archived       int    `json:"archived"`
}

// PushDownstream writes local SCN changes into a downstream vec_shadow_log.
func (s *Service) PushDownstream(ctx context.Context, req PushRequest) error {
	if len(req.Roots) == 0 {
		return fmt.Errorf("no roots specified")
	}
	if req.DownstreamShadow == "" {
		req.DownstreamShadow = "shadow_vec_docs"
	}
	if req.SyncBatch <= 0 {
		req.SyncBatch = 200
	}
	local, err := s.ensureDB(ctx, req.DBPath, false)
	if err != nil {
		return err
	}
	if err := ensureSchema(ctx, local); err != nil {
		return err
	}

	down := req.Downstream
	ownedDownstream := false
	if down == nil {
		if req.DownstreamDriver == "" || req.DownstreamDSN == "" {
			return fmt.Errorf("downstream driver/dsn required")
		}
		down, err = sql.Open(req.DownstreamDriver, req.DownstreamDSN)
		if err != nil {
			return err
		}
		ownedDownstream = true
		defer func() { _ = down.Close() }()
	}
	dialect, batchInsert := detectDownstreamDialect(ctx, down, req.Logf)

	for _, spec := range req.Roots {
		if req.Logf != nil {
			req.Logf("sync root=%s starting", spec.Name)
		}
		if req.Logf != nil {
			if pending, maxSCN, err := localPending(ctx, local, spec.Name, req.DownstreamShadow); err == nil {
				req.Logf("sync root=%s shadow=%s start last_scn=%d pending=%d max_scn=%d", spec.Name, req.DownstreamShadow, pending.lastSCN, pending.count, maxSCN)
			} else {
				req.Logf("sync root=%s shadow=%s start pending=unknown err=%v", spec.Name, req.DownstreamShadow, err)
			}
		}
		if err := pushShadowLog(ctx, local, down, pushConfig{
			DatasetID:   spec.Name,
			ShadowTable: req.DownstreamShadow,
			BatchSize:   req.SyncBatch,
			Logf:        req.Logf,
			Dialect:     dialect,
			BatchInsert: batchInsert,
		}); err != nil {
			return err
		}
		if req.ApplyDownstream {
			if err := applyDownstream(ctx, local, down, applyConfig{
				DatasetID: spec.Name,
				BatchSize: req.SyncBatch,
				Dialect:   dialect,
				Driver:    req.DownstreamDriver,
				Logf:      req.Logf,
			}); err != nil {
				return err
			}
		}
	}
	if ownedDownstream && down != nil {
		_ = down.Close()
	}
	return nil
}

type pushConfig struct {
	DatasetID   string
	ShadowTable string
	BatchSize   int
	Logf        func(format string, args ...any)
	Dialect     *info.Dialect
	BatchInsert bool
}

type pushRow struct {
	id        string
	assetID   sql.NullString
	content   sql.NullString
	meta      sql.NullString
	embedding []byte
	model     sql.NullString
	scn       int64
	archived  int
}

func pushShadowLog(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg pushConfig) error {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	stateShadow := pushShadowPrefix + cfg.ShadowTable
	lastSCN, err := localLastSCN(ctx, local, cfg.DatasetID, stateShadow)
	if err != nil {
		return err
	}
	totalUpdate := 0
	totalDelete := 0
	for {
		rows, err := local.QueryContext(ctx, `SELECT id, asset_id, content, meta, embedding, embedding_model, scn, archived
FROM _vec_emb_docs
WHERE dataset_id = ? AND scn > ?
ORDER BY scn
LIMIT ?`, cfg.DatasetID, lastSCN, cfg.BatchSize)
		if err != nil {
			return err
		}
		var batch []pushRow
		var maxSCN int64
		for rows.Next() {
			var row pushRow
			if err := rows.Scan(&row.id, &row.assetID, &row.content, &row.meta, &row.embedding, &row.model, &row.scn, &row.archived); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, row)
			if row.scn > maxSCN {
				maxSCN = row.scn
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(batch) == 0 {
			if cfg.Logf != nil {
				cfg.Logf("sync dataset=%s shadow=%s up_to_scn=%d (no changes)", cfg.DatasetID, cfg.ShadowTable, lastSCN)
			}
			return nil
		}
		if len(batch) == cfg.BatchSize {
			expanded, err := loadRowsBySCN(ctx, local, cfg.DatasetID, maxSCN)
			if err != nil {
				return err
			}
			batch = mergeBatch(batch, expanded, maxSCN)
		}
		count := 0
		batchUpdate := 0
		batchDelete := 0
		if cfg.BatchInsert {
			if err := execBatchInsert(ctx, downstream, cfg, batch, &count, &batchUpdate, &batchDelete, &totalUpdate, &totalDelete); err != nil {
				return err
			}
		} else {
			if err := execSingleInsert(ctx, downstream, cfg, batch, &count, &batchUpdate, &batchDelete, &totalUpdate, &totalDelete); err != nil {
				return err
			}
		}
		lastSCN = maxSCN
		if err := upsertSyncState(ctx, local, cfg.DatasetID, stateShadow, lastSCN); err != nil {
			return err
		}
		if cfg.Logf != nil {
			cfg.Logf("sync dataset=%s shadow=%s pushed=%d scn<=%d update=%d delete=%d batch_update=%d batch_delete=%d",
				cfg.DatasetID, cfg.ShadowTable, count, lastSCN, totalUpdate, totalDelete, batchUpdate, batchDelete)
		}
	}
}

func execSingleInsert(ctx context.Context, downstream *sql.DB, cfg pushConfig, batch []pushRow, count, batchUpdate, batchDelete, totalUpdate, totalDelete *int) error {
	insertQuery := `INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload, created_at)
VALUES(?,?,?,?,?,?,?)`
	for _, row := range batch {
		payload, err := buildPushPayload(cfg.DatasetID, row.id, row.content, row.meta, row.embedding, row.model, row.scn, row.archived)
		if err != nil {
			return err
		}
		op := "update"
		if row.archived != 0 {
			op = "delete"
		}
		if _, err := downstream.ExecContext(ctx, insertQuery, cfg.DatasetID, cfg.ShadowTable, row.scn, op, row.id, payload, time.Now()); err != nil {
			return err
		}
		if row.archived != 0 {
			*batchDelete = *batchDelete + 1
			*totalDelete = *totalDelete + 1
		} else {
			*batchUpdate = *batchUpdate + 1
			*totalUpdate = *totalUpdate + 1
		}
		*count = *count + 1
	}
	return nil
}

func execBatchInsert(ctx context.Context, downstream *sql.DB, cfg pushConfig, batch []pushRow, count, batchUpdate, batchDelete, totalUpdate, totalDelete *int) error {
	query, args, err := buildBatchInsert(cfg, batch)
	if err != nil {
		return err
	}
	if _, err := downstream.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	for _, row := range batch {
		if row.archived != 0 {
			*batchDelete = *batchDelete + 1
			*totalDelete = *totalDelete + 1
		} else {
			*batchUpdate = *batchUpdate + 1
			*totalUpdate = *totalUpdate + 1
		}
		*count = *count + 1
	}
	return nil
}

func buildBatchInsert(cfg pushConfig, batch []pushRow) (string, []any, error) {
	values := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*7)
	for _, row := range batch {
		payload, err := buildPushPayload(cfg.DatasetID, row.id, row.content, row.meta, row.embedding, row.model, row.scn, row.archived)
		if err != nil {
			return "", nil, err
		}
		op := "update"
		if row.archived != 0 {
			op = "delete"
		}
		values = append(values, "(?,?,?,?,?,?,?)")
		args = append(args, cfg.DatasetID, cfg.ShadowTable, row.scn, op, row.id, payload, time.Now())
	}
	query := "INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload, created_at) VALUES " + strings.Join(values, ",")
	if cfg.Dialect != nil {
		query = cfg.Dialect.EnsurePlaceholders(query)
	}
	return query, args, nil
}

type pendingStats struct {
	lastSCN int64
	count   int64
}

func localPending(ctx context.Context, db *sql.DB, datasetID, shadowTable string) (pendingStats, int64, error) {
	stateShadow := pushShadowPrefix + shadowTable
	lastSCN, err := localLastSCN(ctx, db, datasetID, stateShadow)
	if err != nil {
		return pendingStats{}, 0, err
	}
	var maxSCN int64
	var count int64
	err = db.QueryRowContext(ctx, `SELECT COALESCE(MAX(scn), 0), COUNT(*)
FROM _vec_emb_docs
WHERE dataset_id = ? AND scn > ?`, datasetID, lastSCN).Scan(&maxSCN, &count)
	if err != nil {
		return pendingStats{}, 0, err
	}
	return pendingStats{lastSCN: lastSCN, count: count}, maxSCN, nil
}

func detectDownstreamDialect(ctx context.Context, db *sql.DB, logf func(format string, args ...any)) (*info.Dialect, bool) {
	dialect, err := config.Dialect(ctx, db)
	if err != nil {
		if logf != nil {
			logf("sync: downstream dialect detection failed: %v", err)
		}
		return nil, false
	}
	return dialect, dialect.Insert.MultiValues()
}

type applyConfig struct {
	DatasetID string
	BatchSize int
	Dialect   *info.Dialect
	Driver    string
	Logf      func(format string, args ...any)
}

const applyShadowPrefix = "apply:"

func applyDownstream(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg applyConfig) error {
	driver := strings.ToLower(cfg.Driver)
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	switch driver {
	case "bigquery":
		docsSCN, err := applyShadowDocs(ctx, local, downstream, cfg, buildDocsMerge)
		if err != nil {
			return err
		}
		if err := applyAssets(ctx, local, downstream, cfg, buildAssetMerge); err != nil {
			return err
		}
		if err := applyRoot(ctx, local, downstream, cfg, buildRootMerge); err != nil {
			return err
		}
		if err := applyRootConfig(ctx, local, downstream, cfg, buildRootConfigMerge); err != nil {
			return err
		}
		if err := applyDataset(ctx, local, downstream, cfg, buildDatasetMerge); err != nil {
			return err
		}
		if err := applyDatasetSCN(ctx, local, downstream, cfg, buildDatasetSCNMerge); err != nil {
			return err
		}
		seedSCN, err := seedSCNValue(ctx, local, cfg.DatasetID, docsSCN)
		if err != nil {
			return err
		}
		return seedUpstreamState(ctx, local, cfg.DatasetID, seedSCN, cfg.Logf)
	case "mysql":
		docsSCN, err := applyShadowDocs(ctx, local, downstream, cfg, buildDocsUpsertMySQL)
		if err != nil {
			return err
		}
		if err := applyAssets(ctx, local, downstream, cfg, buildAssetUpsertMySQL); err != nil {
			return err
		}
		if err := applyRoot(ctx, local, downstream, cfg, buildRootUpsertMySQL); err != nil {
			return err
		}
		if err := applyRootConfig(ctx, local, downstream, cfg, buildRootConfigUpsertMySQL); err != nil {
			return err
		}
		if err := applyDataset(ctx, local, downstream, cfg, buildDatasetUpsertMySQL); err != nil {
			return err
		}
		if err := applyDatasetSCN(ctx, local, downstream, cfg, buildDatasetSCNUpsertMySQL); err != nil {
			return err
		}
		seedSCN, err := seedSCNValue(ctx, local, cfg.DatasetID, docsSCN)
		if err != nil {
			return err
		}
		return seedUpstreamState(ctx, local, cfg.DatasetID, seedSCN, cfg.Logf)
	default:
		return fmt.Errorf("apply: downstream driver %q not supported", cfg.Driver)
	}
}

type docsBuilder func(cfg applyConfig, batch []pushRow) (string, []any, error)
type assetBuilder func(cfg applyConfig, batch []assetRow) (string, []any, error)
type rootBuilder func(cfg applyConfig, datasetID string, sourceURI, description sql.NullString, lastIndexed sql.NullTime, lastSCN int64) (string, []any)
type rootConfigBuilder func(cfg applyConfig, datasetID string, includeGlobs, excludeGlobs sql.NullString, maxSize sql.NullInt64, updatedAt sql.NullTime) (string, []any)
type datasetBuilder func(cfg applyConfig, datasetID string, description, sourceURI sql.NullString, lastSCN int64) (string, []any)
type datasetSCNBuilder func(cfg applyConfig, datasetID string, nextSCN int64) (string, []any)

func applyShadowDocs(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg applyConfig, builder docsBuilder) (int64, error) {
	stateShadow := applyShadowPrefix + "shadow_vec_docs"
	lastSCN, err := localLastSCN(ctx, local, cfg.DatasetID, stateShadow)
	if err != nil {
		return 0, err
	}
	for {
		rows, err := local.QueryContext(ctx, `SELECT id, content, meta, embedding, embedding_model, scn, archived
FROM _vec_emb_docs
WHERE dataset_id = ? AND scn > ?
ORDER BY scn
LIMIT ?`, cfg.DatasetID, lastSCN, cfg.BatchSize)
		if err != nil {
			return lastSCN, err
		}
		var batch []pushRow
		var maxSCN int64
		for rows.Next() {
			var row pushRow
			if err := rows.Scan(&row.id, &row.content, &row.meta, &row.embedding, &row.model, &row.scn, &row.archived); err != nil {
				rows.Close()
				return lastSCN, err
			}
			batch = append(batch, row)
			if row.scn > maxSCN {
				maxSCN = row.scn
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return lastSCN, err
		}
		rows.Close()
		if len(batch) == 0 {
			if cfg.Logf != nil {
				cfg.Logf("apply dataset=%s shadow=%s up_to_scn=%d (no changes)", cfg.DatasetID, "shadow_vec_docs", lastSCN)
			}
			return lastSCN, nil
		}
		if len(batch) == cfg.BatchSize {
			expanded, err := loadRowsBySCN(ctx, local, cfg.DatasetID, maxSCN)
			if err != nil {
				return lastSCN, err
			}
			batch = mergeBatch(batch, expanded, maxSCN)
		}
		query, args, err := builder(cfg, batch)
		if err != nil {
			return lastSCN, err
		}
		if _, err := downstream.ExecContext(ctx, query, args...); err != nil {
			return lastSCN, err
		}
		lastSCN = maxSCN
		if err := upsertSyncState(ctx, local, cfg.DatasetID, stateShadow, lastSCN); err != nil {
			return lastSCN, err
		}
		if cfg.Logf != nil {
			cfg.Logf("apply dataset=%s shadow=%s applied=%d scn<=%d", cfg.DatasetID, "shadow_vec_docs", len(batch), lastSCN)
		}
	}
}

func seedUpstreamState(ctx context.Context, local *sql.DB, datasetID string, lastSCN int64, logf func(format string, args ...any)) error {
	if err := upsertSyncState(ctx, local, datasetID, "shadow_vec_docs", lastSCN); err != nil {
		return err
	}
	if logf != nil {
		logf("sync dataset=%s shadow=%s seeded last_scn=%d", datasetID, "shadow_vec_docs", lastSCN)
	}
	return nil
}

func seedSCNValue(ctx context.Context, local *sql.DB, datasetID string, appliedSCN int64) (int64, error) {
	if appliedSCN > 0 {
		return appliedSCN, nil
	}
	maxSCN, err := localDocsMaxSCN(ctx, local, datasetID)
	if err != nil {
		return 0, err
	}
	return maxSCN, nil
}

func localDocsMaxSCN(ctx context.Context, db *sql.DB, datasetID string) (int64, error) {
	var maxSCN int64
	err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(scn), 0) FROM _vec_emb_docs WHERE dataset_id = ?`, datasetID).Scan(&maxSCN)
	if err != nil {
		return 0, err
	}
	return maxSCN, nil
}

type assetRow struct {
	assetID  string
	path     string
	md5      string
	size     int64
	modTime  time.Time
	scn      int64
	archived int
}

func applyAssets(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg applyConfig, builder assetBuilder) error {
	stateShadow := applyShadowPrefix + "emb_asset"
	lastSCN, err := localLastSCN(ctx, local, cfg.DatasetID, stateShadow)
	if err != nil {
		return err
	}
	for {
		rows, err := local.QueryContext(ctx, `SELECT asset_id, path, md5, size, mod_time, scn, archived
FROM emb_asset
WHERE dataset_id = ? AND scn > ?
ORDER BY scn
LIMIT ?`, cfg.DatasetID, lastSCN, cfg.BatchSize)
		if err != nil {
			return err
		}
		var batch []assetRow
		var maxSCN int64
		for rows.Next() {
			var row assetRow
			if err := rows.Scan(&row.assetID, &row.path, &row.md5, &row.size, &row.modTime, &row.scn, &row.archived); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, row)
			if row.scn > maxSCN {
				maxSCN = row.scn
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(batch) == 0 {
			if cfg.Logf != nil {
				cfg.Logf("apply dataset=%s shadow=%s up_to_scn=%d (no changes)", cfg.DatasetID, "emb_asset", lastSCN)
			}
			return nil
		}
		query, args, err := builder(cfg, batch)
		if err != nil {
			return err
		}
		if _, err := downstream.ExecContext(ctx, query, args...); err != nil {
			return err
		}
		lastSCN = maxSCN
		if err := upsertSyncState(ctx, local, cfg.DatasetID, stateShadow, lastSCN); err != nil {
			return err
		}
		if cfg.Logf != nil {
			cfg.Logf("apply dataset=%s shadow=%s applied=%d scn<=%d", cfg.DatasetID, "emb_asset", len(batch), lastSCN)
		}
	}
}

func applyRoot(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg applyConfig, builder rootBuilder) error {
	row := local.QueryRowContext(ctx, `SELECT source_uri, description, last_indexed_at, last_scn FROM emb_root WHERE dataset_id = ?`, cfg.DatasetID)
	var sourceURI, description sql.NullString
	var lastIndexed sql.NullTime
	var lastSCN int64
	if err := row.Scan(&sourceURI, &description, &lastIndexed, &lastSCN); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	query, args := builder(cfg, cfg.DatasetID, sourceURI, description, lastIndexed, lastSCN)
	if _, err := downstream.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	if cfg.Logf != nil {
		cfg.Logf("apply dataset=%s shadow=%s applied=1 scn=%d", cfg.DatasetID, "emb_root", lastSCN)
	}
	return nil
}

func applyRootConfig(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg applyConfig, builder rootConfigBuilder) error {
	row := local.QueryRowContext(ctx, `SELECT include_globs, exclude_globs, max_size_bytes, updated_at FROM emb_root_config WHERE dataset_id = ?`, cfg.DatasetID)
	var includeGlobs, excludeGlobs sql.NullString
	var maxSize sql.NullInt64
	var updatedAt sql.NullTime
	if err := row.Scan(&includeGlobs, &excludeGlobs, &maxSize, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	query, args := builder(cfg, cfg.DatasetID, includeGlobs, excludeGlobs, maxSize, updatedAt)
	if _, err := downstream.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	if cfg.Logf != nil {
		cfg.Logf("apply dataset=%s shadow=%s applied=1", cfg.DatasetID, "emb_root_config")
	}
	return nil
}

func buildDocsMerge(cfg applyConfig, batch []pushRow) (string, []any, error) {
	selects := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*8)
	for _, row := range batch {
		selects = append(selects, "SELECT ? AS dataset_id, ? AS id, ? AS content, ? AS meta, ? AS embedding, ? AS embedding_model, ? AS scn, ? AS archived")
		archived := row.archived != 0
		args = append(args, cfg.DatasetID, row.id, row.content.String, row.meta.String, row.embedding, row.model.String, row.scn, archived)
	}
	query := `MERGE shadow_vec_docs T
USING (` + strings.Join(selects, " UNION ALL ") + `) S
ON T.dataset_id = S.dataset_id AND T.id = S.id
WHEN MATCHED THEN UPDATE SET content = S.content, meta = S.meta, embedding = S.embedding, embedding_model = S.embedding_model, scn = S.scn, archived = S.archived
WHEN NOT MATCHED THEN INSERT (dataset_id, id, content, meta, embedding, embedding_model, scn, archived)
VALUES (S.dataset_id, S.id, S.content, S.meta, S.embedding, S.embedding_model, S.scn, S.archived)`
	if cfg.Dialect != nil {
		query = cfg.Dialect.EnsurePlaceholders(query)
	}
	return query, args, nil
}

func buildAssetMerge(cfg applyConfig, batch []assetRow) (string, []any, error) {
	selects := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*8)
	for _, row := range batch {
		selects = append(selects, "SELECT ? AS dataset_id, ? AS asset_id, ? AS path, ? AS md5, ? AS size, ? AS mod_time, ? AS scn, ? AS archived")
		archived := row.archived != 0
		modTime := any(row.modTime)
		if strings.EqualFold(cfg.Driver, "bigquery") {
			modTime = formatBQTimestamp(row.modTime)
		}
		if strings.EqualFold(cfg.Driver, "bigquery") {
			selects[len(selects)-1] = "SELECT ? AS dataset_id, ? AS asset_id, ? AS path, ? AS md5, ? AS size, TIMESTAMP(?) AS mod_time, ? AS scn, ? AS archived"
		}
		args = append(args, cfg.DatasetID, row.assetID, row.path, row.md5, row.size, modTime, row.scn, archived)
	}
	query := `MERGE emb_asset T
USING (` + strings.Join(selects, " UNION ALL ") + `) S
ON T.dataset_id = S.dataset_id AND T.asset_id = S.asset_id
WHEN MATCHED THEN UPDATE SET path = S.path, md5 = S.md5, size = S.size, mod_time = S.mod_time, scn = S.scn, archived = S.archived
WHEN NOT MATCHED THEN INSERT (dataset_id, asset_id, path, md5, size, mod_time, scn, archived)
VALUES (S.dataset_id, S.asset_id, S.path, S.md5, S.size, S.mod_time, S.scn, S.archived)`
	if cfg.Dialect != nil {
		query = cfg.Dialect.EnsurePlaceholders(query)
	}
	return query, args, nil
}

func buildDocsUpsertMySQL(cfg applyConfig, batch []pushRow) (string, []any, error) {
	values := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*8)
	for _, row := range batch {
		values = append(values, "(?,?,?,?,?,?,?,?)")
		args = append(args, cfg.DatasetID, row.id, row.content.String, row.meta.String, row.embedding, row.model.String, row.scn, row.archived)
	}
	query := `INSERT INTO shadow_vec_docs (dataset_id, id, content, meta, embedding, embedding_model, scn, archived)
VALUES ` + strings.Join(values, ",") + `
ON DUPLICATE KEY UPDATE
	content=VALUES(content),
	meta=VALUES(meta),
	embedding=VALUES(embedding),
	embedding_model=VALUES(embedding_model),
	scn=VALUES(scn),
	archived=VALUES(archived)`
	return query, args, nil
}

func buildAssetUpsertMySQL(cfg applyConfig, batch []assetRow) (string, []any, error) {
	values := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*8)
	for _, row := range batch {
		values = append(values, "(?,?,?,?,?,?,?,?)")
		args = append(args, cfg.DatasetID, row.assetID, row.path, row.md5, row.size, row.modTime, row.scn, row.archived)
	}
	query := `INSERT INTO emb_asset (dataset_id, asset_id, path, md5, size, mod_time, scn, archived)
VALUES ` + strings.Join(values, ",") + `
ON DUPLICATE KEY UPDATE
	path=VALUES(path),
	md5=VALUES(md5),
	size=VALUES(size),
	mod_time=VALUES(mod_time),
	scn=VALUES(scn),
	archived=VALUES(archived)`
	return query, args, nil
}

func buildRootMerge(cfg applyConfig, datasetID string, sourceURI, description sql.NullString, lastIndexed sql.NullTime, lastSCN int64) (string, []any) {
	query := `MERGE emb_root T
USING (SELECT ? AS dataset_id, ? AS source_uri, ? AS description, %s AS last_indexed_at, ? AS last_scn) S
ON T.dataset_id = S.dataset_id
WHEN MATCHED THEN UPDATE SET source_uri = S.source_uri, description = S.description, last_indexed_at = S.last_indexed_at, last_scn = S.last_scn
WHEN NOT MATCHED THEN INSERT (dataset_id, source_uri, description, last_indexed_at, last_scn)
VALUES (S.dataset_id, S.source_uri, S.description, S.last_indexed_at, S.last_scn)`
	lastIndexedExpr := "?"
	if cfg.Dialect != nil {
		if strings.EqualFold(cfg.Driver, "bigquery") {
			lastIndexedExpr = "TIMESTAMP(?)"
		}
		query = cfg.Dialect.EnsurePlaceholders(fmt.Sprintf(query, lastIndexedExpr))
	}
	var lastIndexedVal any
	if lastIndexed.Valid {
		lastIndexedVal = lastIndexed.Time
		if strings.EqualFold(cfg.Driver, "bigquery") {
			lastIndexedVal = formatBQTimestamp(lastIndexed.Time)
		}
	} else {
		lastIndexedVal = nil
	}
	return query, []any{datasetID, sourceURI.String, description.String, lastIndexedVal, lastSCN}
}

func buildRootUpsertMySQL(cfg applyConfig, datasetID string, sourceURI, description sql.NullString, lastIndexed sql.NullTime, lastSCN int64) (string, []any) {
	query := `INSERT INTO emb_root (dataset_id, source_uri, description, last_indexed_at, last_scn)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
	source_uri=VALUES(source_uri),
	description=VALUES(description),
	last_indexed_at=VALUES(last_indexed_at),
	last_scn=VALUES(last_scn)`
	return query, []any{datasetID, sourceURI.String, description.String, lastIndexed.Time, lastSCN}
}

func buildRootConfigMerge(cfg applyConfig, datasetID string, includeGlobs, excludeGlobs sql.NullString, maxSize sql.NullInt64, updatedAt sql.NullTime) (string, []any) {
	query := `MERGE emb_root_config T
USING (SELECT ? AS dataset_id, ? AS include_globs, ? AS exclude_globs, ? AS max_size_bytes, %s AS updated_at) S
ON T.dataset_id = S.dataset_id
WHEN MATCHED THEN UPDATE SET include_globs = S.include_globs, exclude_globs = S.exclude_globs, max_size_bytes = S.max_size_bytes, updated_at = S.updated_at
WHEN NOT MATCHED THEN INSERT (dataset_id, include_globs, exclude_globs, max_size_bytes, updated_at)
VALUES (S.dataset_id, S.include_globs, S.exclude_globs, S.max_size_bytes, S.updated_at)`
	updatedAtExpr := "?"
	if cfg.Dialect != nil {
		if strings.EqualFold(cfg.Driver, "bigquery") {
			updatedAtExpr = "TIMESTAMP(?)"
		}
		query = cfg.Dialect.EnsurePlaceholders(fmt.Sprintf(query, updatedAtExpr))
	}
	var updatedAtVal any
	if updatedAt.Valid {
		updatedAtVal = updatedAt.Time
		if strings.EqualFold(cfg.Driver, "bigquery") {
			updatedAtVal = formatBQTimestamp(updatedAt.Time)
		}
	} else {
		updatedAtVal = nil
	}
	return query, []any{datasetID, includeGlobs.String, excludeGlobs.String, maxSize.Int64, updatedAtVal}
}

func buildRootConfigUpsertMySQL(cfg applyConfig, datasetID string, includeGlobs, excludeGlobs sql.NullString, maxSize sql.NullInt64, updatedAt sql.NullTime) (string, []any) {
	query := `INSERT INTO emb_root_config (dataset_id, include_globs, exclude_globs, max_size_bytes, updated_at)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
	include_globs=VALUES(include_globs),
	exclude_globs=VALUES(exclude_globs),
	max_size_bytes=VALUES(max_size_bytes),
	updated_at=VALUES(updated_at)`
	var updatedAtVal any
	if updatedAt.Valid {
		updatedAtVal = updatedAt.Time
	} else {
		updatedAtVal = nil
	}
	return query, []any{datasetID, includeGlobs.String, excludeGlobs.String, maxSize.Int64, updatedAtVal}
}

func formatBQTimestamp(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.999999")
}

func applyDataset(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg applyConfig, builder datasetBuilder) error {
	row := local.QueryRowContext(ctx, `SELECT description, source_uri, last_scn FROM vec_dataset WHERE dataset_id = ?`, cfg.DatasetID)
	var description, sourceURI sql.NullString
	var lastSCN int64
	if err := row.Scan(&description, &sourceURI, &lastSCN); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	query, args := builder(cfg, cfg.DatasetID, description, sourceURI, lastSCN)
	if _, err := downstream.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	if cfg.Logf != nil {
		cfg.Logf("apply dataset=%s shadow=%s applied=1 scn=%d", cfg.DatasetID, "vec_dataset", lastSCN)
	}
	return nil
}

func applyDatasetSCN(ctx context.Context, local *sql.DB, downstream *sql.DB, cfg applyConfig, builder datasetSCNBuilder) error {
	row := local.QueryRowContext(ctx, `SELECT next_scn FROM vec_dataset_scn WHERE dataset_id = ?`, cfg.DatasetID)
	var nextSCN int64
	if err := row.Scan(&nextSCN); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	query, args := builder(cfg, cfg.DatasetID, nextSCN)
	if _, err := downstream.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	if cfg.Logf != nil {
		cfg.Logf("apply dataset=%s shadow=%s applied=1 next_scn=%d", cfg.DatasetID, "vec_dataset_scn", nextSCN)
	}
	return nil
}

func buildDatasetMerge(cfg applyConfig, datasetID string, description, sourceURI sql.NullString, lastSCN int64) (string, []any) {
	query := `MERGE vec_dataset T
USING (SELECT ? AS dataset_id, ? AS description, ? AS source_uri, ? AS last_scn) S
ON T.dataset_id = S.dataset_id
WHEN MATCHED THEN UPDATE SET description = S.description, source_uri = S.source_uri, last_scn = S.last_scn
WHEN NOT MATCHED THEN INSERT (dataset_id, description, source_uri, last_scn)
VALUES (S.dataset_id, S.description, S.source_uri, S.last_scn)`
	if cfg.Dialect != nil {
		query = cfg.Dialect.EnsurePlaceholders(query)
	}
	return query, []any{datasetID, description.String, sourceURI.String, lastSCN}
}

func buildDatasetSCNMerge(cfg applyConfig, datasetID string, nextSCN int64) (string, []any) {
	query := `MERGE vec_dataset_scn T
USING (SELECT ? AS dataset_id, ? AS next_scn) S
ON T.dataset_id = S.dataset_id
WHEN MATCHED THEN UPDATE SET next_scn = S.next_scn
WHEN NOT MATCHED THEN INSERT (dataset_id, next_scn)
VALUES (S.dataset_id, S.next_scn)`
	if cfg.Dialect != nil {
		query = cfg.Dialect.EnsurePlaceholders(query)
	}
	return query, []any{datasetID, nextSCN}
}

func buildDatasetUpsertMySQL(cfg applyConfig, datasetID string, description, sourceURI sql.NullString, lastSCN int64) (string, []any) {
	query := `INSERT INTO vec_dataset (dataset_id, description, source_uri, last_scn)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
	description=VALUES(description),
	source_uri=VALUES(source_uri),
	last_scn=VALUES(last_scn)`
	return query, []any{datasetID, description.String, sourceURI.String, lastSCN}
}

func buildDatasetSCNUpsertMySQL(cfg applyConfig, datasetID string, nextSCN int64) (string, []any) {
	query := `INSERT INTO vec_dataset_scn (dataset_id, next_scn)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
	next_scn=VALUES(next_scn)`
	return query, []any{datasetID, nextSCN}
}

func loadRowsBySCN(ctx context.Context, db *sql.DB, datasetID string, scn int64) ([]pushRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, asset_id, content, meta, embedding, embedding_model, scn, archived
FROM _vec_emb_docs
WHERE dataset_id = ? AND scn = ?
ORDER BY id`, datasetID, scn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pushRow
	for rows.Next() {
		var row pushRow
		if err := rows.Scan(&row.id, &row.assetID, &row.content, &row.meta, &row.embedding, &row.model, &row.scn, &row.archived); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func mergeBatch(batch []pushRow, expanded []pushRow, maxSCN int64) []pushRow {
	if len(expanded) == 0 {
		return batch
	}
	out := make([]pushRow, 0, len(batch)+len(expanded))
	seen := map[string]struct{}{}
	for _, row := range batch {
		if row.scn == maxSCN {
			seen[row.id] = struct{}{}
			continue
		}
		out = append(out, row)
	}
	for _, row := range expanded {
		if _, ok := seen[row.id]; ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

func buildPushPayload(datasetID, id string, content, meta sql.NullString, embedding []byte, model sql.NullString, scn int64, archived int) ([]byte, error) {
	payload := pushPayload{
		DatasetID:      datasetID,
		ID:             id,
		Content:        content.String,
		Meta:           meta.String,
		Embedding:      encodeHexBlob(embedding),
		EmbeddingModel: model.String,
		SCN:            scn,
		Archived:       archived,
	}
	if !content.Valid {
		payload.Content = ""
	}
	if !meta.Valid {
		payload.Meta = ""
	}
	if !model.Valid {
		payload.EmbeddingModel = ""
	}
	return json.Marshal(payload)
}

func encodeHexBlob(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return strings.ToLower(hex.EncodeToString(b))
}

func localLastSCN(ctx context.Context, db *sql.DB, datasetID, shadowTable string) (int64, error) {
	var last int64
	err := db.QueryRowContext(ctx, `SELECT last_scn FROM vec_sync_state WHERE dataset_id = ? AND shadow_table = ?`, datasetID, shadowTable).Scan(&last)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return last, err
}

func upsertSyncState(ctx context.Context, db *sql.DB, datasetID, shadowTable string, scn int64) error {
	_, err := db.ExecContext(ctx, `INSERT INTO vec_sync_state(dataset_id, shadow_table, last_scn, updated_at)
VALUES(?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(dataset_id, shadow_table) DO UPDATE SET last_scn=excluded.last_scn, updated_at=CURRENT_TIMESTAMP`, datasetID, shadowTable, scn)
	return err
}
