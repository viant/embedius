package sqlitevec

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// SyncConfig controls upstream SCN synchronization into the local sqlite-vec store.
type SyncConfig struct {
	DatasetID      string
	UpstreamShadow string // upstream vec_shadow_log.shadow_table
	LocalShadow    string // local shadow table (e.g. _vec_emb_docs)
	AssetTable     string // local asset table (e.g. emb_asset)
	BatchSize      int
	Invalidate     bool // call vec_invalidate(shadow, dataset) after applying a batch
	Logf           func(format string, args ...any)
	// Filter returns true to apply a log entry for the given path/meta.
	// Return false to skip applying the entry (SCN still advances).
	Filter func(path string, meta string) bool
}

// SyncUpstream pulls SCN log entries from an upstream database and applies them
// to the local sqlite-vec shadow table and asset table.
func SyncUpstream(ctx context.Context, local *sql.DB, upstream *sql.DB, cfg SyncConfig) error {
	if local == nil || upstream == nil {
		return fmt.Errorf("sync: local and upstream db are required")
	}
	if cfg.DatasetID == "" {
		return fmt.Errorf("sync: dataset_id is required")
	}
	if cfg.UpstreamShadow == "" {
		cfg.UpstreamShadow = "shadow_vec_docs"
	}
	if cfg.LocalShadow == "" {
		cfg.LocalShadow = "_vec_emb_docs"
	}
	if cfg.AssetTable == "" {
		cfg.AssetTable = "emb_asset"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}

	lastSCN, err := localLastSCN(ctx, local, cfg.DatasetID, cfg.UpstreamShadow)
	if err != nil {
		return err
	}
	if cfg.Logf != nil {
		if pending, maxSCN, err := upstreamPending(ctx, upstream, cfg.DatasetID, cfg.UpstreamShadow, lastSCN); err == nil {
			cfg.Logf("sync dataset=%s shadow=%s start last_scn=%d pending=%d max_scn=%d", cfg.DatasetID, cfg.UpstreamShadow, lastSCN, pending, maxSCN)
		} else {
			cfg.Logf("sync dataset=%s shadow=%s start last_scn=%d pending=unknown err=%v", cfg.DatasetID, cfg.UpstreamShadow, lastSCN, err)
		}
	}

	totalInsert := 0
	totalUpdate := 0
	totalDelete := 0
	for {
		batchInsert := 0
		batchUpdate := 0
		batchDelete := 0
		rows, err := upstream.QueryContext(ctx, `SELECT dataset_id, shadow_table, scn, op, document_id, payload
FROM vec_shadow_log
WHERE dataset_id = ? AND shadow_table = ? AND scn > ?
ORDER BY scn
LIMIT ?`, cfg.DatasetID, cfg.UpstreamShadow, lastSCN, cfg.BatchSize)
		if err != nil {
			return err
		}
		var maxSCN int64
		var minSCN int64
		count := 0
		for rows.Next() {
			var ds, st, op, docID string
			var scn int64
			var payload []byte
			if err := rows.Scan(&ds, &st, &scn, &op, &docID, &payload); err != nil {
				rows.Close()
				return err
			}
			switch strings.ToLower(op) {
			case "insert":
				batchInsert++
				totalInsert++
			case "delete":
				batchDelete++
				totalDelete++
			default:
				batchUpdate++
				totalUpdate++
			}
			if err := applyLogEntry(ctx, local, cfg, op, docID, payload, scn); err != nil {
				rows.Close()
				return err
			}
			if minSCN == 0 || scn < minSCN {
				minSCN = scn
			}
			if scn > maxSCN {
				maxSCN = scn
			}
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if count == 0 {
			if cfg.Logf != nil {
				cfg.Logf("sync dataset=%s shadow=%s up_to_scn=%d (no changes)", cfg.DatasetID, cfg.UpstreamShadow, lastSCN)
			}
			return nil
		}
		lastSCN = maxSCN
		if err := upsertSyncState(ctx, local, cfg.DatasetID, cfg.UpstreamShadow, lastSCN); err != nil {
			return err
		}
		if cfg.Invalidate {
			_, _ = local.ExecContext(ctx, `SELECT vec_invalidate(?, ?)`, cfg.LocalShadow, cfg.DatasetID)
		}
		if cfg.Logf != nil {
			cfg.Logf("sync dataset=%s shadow=%s applied=%d scn=%d..%d insert=%d update=%d delete=%d total_insert=%d total_update=%d total_delete=%d",
				cfg.DatasetID, cfg.UpstreamShadow, count, minSCN, maxSCN, batchInsert, batchUpdate, batchDelete, totalInsert, totalUpdate, totalDelete)
		}
	}
}

func upstreamPending(ctx context.Context, upstream *sql.DB, datasetID, shadowTable string, lastSCN int64) (int64, int64, error) {
	var maxSCN int64
	var pending int64
	err := upstream.QueryRowContext(ctx, `SELECT COALESCE(MAX(scn), 0), COUNT(*)
FROM vec_shadow_log
WHERE dataset_id = ? AND shadow_table = ? AND scn > ?`, datasetID, shadowTable, lastSCN).Scan(&maxSCN, &pending)
	return pending, maxSCN, err
}

type logPayload struct {
	DatasetID      string `json:"dataset_id"`
	ID             string `json:"id"`
	Content        string `json:"content"`
	Meta           string `json:"meta"`
	Embedding      string `json:"embedding"`
	EmbeddingModel string `json:"embedding_model"`
	SCN            int64  `json:"scn"`
	Archived       int    `json:"archived"`
}

func applyLogEntry(ctx context.Context, db *sql.DB, cfg SyncConfig, op, docID string, payload []byte, scn int64) error {
	var p logPayload
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
	}
	if p.ID == "" {
		p.ID = docID
	}
	assetID, relPath, md5hex := assetFromMeta(p.Meta, p.ID)

	if op == "delete" {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET archived=1, scn=? WHERE dataset_id=? AND id=?`, cfg.LocalShadow), scn, cfg.DatasetID, p.ID); err != nil {
			return err
		}
		if assetID != "" {
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET archived=1, scn=? WHERE dataset_id=? AND asset_id=?`, cfg.AssetTable), scn, cfg.DatasetID, assetID); err != nil {
				return err
			}
		}
		return nil
	}
	if cfg.Filter != nil && !cfg.Filter(relPath, p.Meta) {
		return nil
	}

	embBlob, err := decodeHexBlob(p.Embedding)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s(dataset_id, id, asset_id, content, meta, embedding, embedding_model, scn, archived)
VALUES(?,?,?,?,?,?,?,?,?)
ON CONFLICT(dataset_id, id) DO UPDATE SET
	asset_id=excluded.asset_id,
	content=excluded.content,
	meta=excluded.meta,
	embedding=excluded.embedding,
	embedding_model=excluded.embedding_model,
	scn=excluded.scn,
	archived=excluded.archived`, cfg.LocalShadow), cfg.DatasetID, p.ID, assetID, p.Content, p.Meta, embBlob, p.EmbeddingModel, scn, p.Archived); err != nil {
		return err
	}

	if assetID != "" && relPath != "" && md5hex != "" {
		_, _ = db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s(dataset_id, asset_id, path, md5, size, mod_time, scn, archived)
VALUES(?,?,?,?,0,CURRENT_TIMESTAMP,?,0)
ON CONFLICT(dataset_id, asset_id) DO UPDATE SET
	path=excluded.path,
	md5=excluded.md5,
	scn=excluded.scn,
	archived=0`, cfg.AssetTable), cfg.DatasetID, assetID, relPath, md5hex, scn)
	}
	return nil
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

func decodeHexBlob(hexStr string) ([]byte, error) {
	if hexStr == "" {
		return nil, nil
	}
	return hex.DecodeString(hexStr)
}

func assetFromMeta(metaStr, docID string) (assetID, relPath, md5hex string) {
	if metaStr != "" {
		var meta map[string]interface{}
		if err := json.Unmarshal([]byte(metaStr), &meta); err == nil {
			if v, ok := meta["asset_id"].(string); ok && v != "" {
				assetID = v
			}
			if v, ok := meta["path"].(string); ok && v != "" {
				relPath = v
			}
			if v, ok := meta["md5"].(string); ok && v != "" {
				md5hex = v
			}
		}
	}
	if assetID == "" {
		if i := strings.Index(docID, "#"); i > 0 {
			assetID = docID[:i]
		} else {
			assetID = docID
		}
	}
	if relPath == "" {
		relPath = assetID
	}
	return assetID, relPath, md5hex
}
