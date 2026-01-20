package sqlitevec

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vector"
)

type syncPayload struct {
	DatasetID      string `json:"dataset_id"`
	ID             string `json:"id"`
	Content        string `json:"content"`
	Meta           string `json:"meta"`
	Embedding      string `json:"embedding"`
	EmbeddingModel string `json:"embedding_model"`
	SCN            int64  `json:"scn"`
	Archived       int    `json:"archived"`
}

func TestSyncUpstream_AppliesLogEntries(t *testing.T) {
	ctx := context.Background()
	local, err := NewStore(WithDSN(":memory:"), WithEnsureSchema(true))
	if err != nil {
		t.Fatalf("local store init: %v", err)
	}
	defer local.Close()

	upstream, err := engine.Open(":memory:")
	if err != nil {
		t.Fatalf("upstream open: %v", err)
	}
	defer upstream.Close()
	upstream.SetMaxOpenConns(1)

	if _, err := upstream.Exec(`CREATE TABLE vec_shadow_log (
		dataset_id   TEXT NOT NULL,
		shadow_table TEXT NOT NULL,
		scn          INTEGER NOT NULL,
		op           TEXT NOT NULL,
		document_id  TEXT NOT NULL,
		payload      BLOB NOT NULL,
		created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`); err != nil {
		t.Fatalf("create vec_shadow_log: %v", err)
	}

	emb, err := vector.EncodeEmbedding([]float32{1, 2, 3})
	if err != nil {
		t.Fatalf("encode embedding: %v", err)
	}
	payload := func(content string, scn int64, archived int) []byte {
		meta := map[string]interface{}{
			"asset_id": "asset-1",
			"path":     "docs/a.txt",
			"md5":      "deadbeef",
		}
		metaJSON, _ := json.Marshal(meta)
		p := syncPayload{
			DatasetID:      "rootA",
			ID:             "asset-1#0",
			Content:        content,
			Meta:           string(metaJSON),
			Embedding:      hex.EncodeToString(emb),
			EmbeddingModel: "text-embedding-3-small",
			SCN:            scn,
			Archived:       archived,
		}
		b, _ := json.Marshal(&p)
		return b
	}

	on := func(scn int64, op string, content string, archived int) {
		if _, err := upstream.Exec(`INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload, created_at)
			VALUES(?,?,?,?,?,?,?)`, "rootA", "shadow_vec_docs", scn, op, "asset-1#0", payload(content, scn, archived), time.Now()); err != nil {
			t.Fatalf("insert log: %v", err)
		}
	}

	on(1, "insert", "alpha", 0)
	on(2, "update", "beta", 0)
	on(3, "delete", "beta", 1)

	cfg := SyncConfig{
		DatasetID:      "rootA",
		UpstreamShadow: "shadow_vec_docs",
		LocalShadow:    "_vec_emb_docs",
		AssetTable:     "emb_asset",
		BatchSize:      10,
		Invalidate:     false,
	}
	if err := SyncUpstream(ctx, local.DB(), upstream, cfg); err != nil {
		t.Fatalf("SyncUpstream: %v", err)
	}

	var content, metaJSON, embModel string
	var archived int
	var scn int64
	err = local.DB().QueryRow(`SELECT content, meta, embedding_model, scn, archived FROM _vec_emb_docs WHERE dataset_id = ? AND id = ?`, "rootA", "asset-1#0").Scan(&content, &metaJSON, &embModel, &scn, &archived)
	if err != nil {
		t.Fatalf("query doc: %v", err)
	}
	if content != "beta" {
		t.Fatalf("expected updated content, got %q", content)
	}
	if scn != 3 {
		t.Fatalf("expected scn=3, got %d", scn)
	}
	if archived != 1 {
		t.Fatalf("expected archived=1, got %d", archived)
	}
	if embModel != "text-embedding-3-small" {
		t.Fatalf("unexpected embedding_model: %q", embModel)
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
		t.Fatalf("meta json: %v", err)
	}
	if meta["asset_id"] != "asset-1" {
		t.Fatalf("unexpected asset_id: %v", meta["asset_id"])
	}

	var assetArchived int
	var assetMD5 string
	err = local.DB().QueryRow(`SELECT archived, md5 FROM emb_asset WHERE dataset_id = ? AND asset_id = ?`, "rootA", "asset-1").Scan(&assetArchived, &assetMD5)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("query asset: %v", err)
	}
	if assetArchived != 1 {
		t.Fatalf("expected asset archived=1, got %d", assetArchived)
	}
	if assetMD5 != "deadbeef" {
		t.Fatalf("unexpected asset md5: %q", assetMD5)
	}

	var lastSCN int64
	if err := local.DB().QueryRow(`SELECT last_scn FROM vec_sync_state WHERE dataset_id = ? AND shadow_table = ?`, "rootA", "shadow_vec_docs").Scan(&lastSCN); err != nil {
		t.Fatalf("sync state: %v", err)
	}
	if lastSCN != 3 {
		t.Fatalf("expected last_scn=3, got %d", lastSCN)
	}
}

func TestSyncUpstream_FilterInsertUpdateOnly(t *testing.T) {
	ctx := context.Background()
	local, err := NewStore(WithDSN(":memory:"), WithEnsureSchema(true))
	if err != nil {
		t.Fatalf("local store init: %v", err)
	}
	defer local.Close()

	upstream, err := engine.Open(":memory:")
	if err != nil {
		t.Fatalf("upstream open: %v", err)
	}
	defer upstream.Close()
	upstream.SetMaxOpenConns(1)

	if _, err := upstream.Exec(`CREATE TABLE vec_shadow_log (
		dataset_id   TEXT NOT NULL,
		shadow_table TEXT NOT NULL,
		scn          INTEGER NOT NULL,
		op           TEXT NOT NULL,
		document_id  TEXT NOT NULL,
		payload      BLOB NOT NULL,
		created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`); err != nil {
		t.Fatalf("create vec_shadow_log: %v", err)
	}

	emb, err := vector.EncodeEmbedding([]float32{1, 2, 3})
	if err != nil {
		t.Fatalf("encode embedding: %v", err)
	}
	meta := map[string]interface{}{"asset_id": "asset-1", "path": "docs/a.txt"}
	metaJSON, _ := json.Marshal(meta)
	payload := func(content string, scn int64, archived int) []byte {
		p := syncPayload{
			DatasetID:      "rootB",
			ID:             "asset-1#0",
			Content:        content,
			Meta:           string(metaJSON),
			Embedding:      hex.EncodeToString(emb),
			EmbeddingModel: "text-embedding-3-small",
			SCN:            scn,
			Archived:       archived,
		}
		b, _ := json.Marshal(&p)
		return b
	}

	insertLog := func(scn int64, op string, content string, archived int) {
		if _, err := upstream.Exec(`INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload, created_at)
			VALUES(?,?,?,?,?,?,?)`, "rootB", "shadow_vec_docs", scn, op, "asset-1#0", payload(content, scn, archived), time.Now()); err != nil {
			t.Fatalf("insert log: %v", err)
		}
	}

	insertLog(1, "insert", "alpha", 0)
	cfg := SyncConfig{
		DatasetID:      "rootB",
		UpstreamShadow: "shadow_vec_docs",
		LocalShadow:    "_vec_emb_docs",
		AssetTable:     "emb_asset",
		BatchSize:      10,
		Invalidate:     false,
		Filter: func(path, meta string) bool {
			return false
		},
	}
	if err := SyncUpstream(ctx, local.DB(), upstream, cfg); err != nil {
		t.Fatalf("SyncUpstream: %v", err)
	}

	var cnt int
	if err := local.DB().QueryRow(`SELECT COUNT(*) FROM _vec_emb_docs WHERE dataset_id = ?`, "rootB").Scan(&cnt); err != nil {
		t.Fatalf("count docs: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected no docs when filtered, got %d", cnt)
	}

	// Apply delete; it should still be applied even if filter blocks inserts.
	insertLog(2, "delete", "alpha", 1)
	if err := SyncUpstream(ctx, local.DB(), upstream, cfg); err != nil {
		t.Fatalf("SyncUpstream delete: %v", err)
	}
	var archived int
	err = local.DB().QueryRow(`SELECT archived FROM _vec_emb_docs WHERE dataset_id = ? AND id = ?`, "rootB", "asset-1#0").Scan(&archived)
	if err != sql.ErrNoRows {
		if err != nil {
			t.Fatalf("query doc: %v", err)
		}
		if archived != 1 {
			t.Fatalf("expected archived=1, got %d", archived)
		}
	}
}
