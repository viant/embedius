package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vector"
)

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

func TestCLIFlow_IndexSearchSyncAdmin(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootDir, "a.txt"), []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "b.txt"), []byte("auth flow and permissions"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	localDB := filepath.Join(t.TempDir(), "embedius.sqlite")
	fmt.Println("indexCmd start")
	indexCmd([]string{"--db", localDB, "--root", "rootA", "--path", rootDir, "--embedder", "simple"})
	fmt.Println("indexCmd done")

	ldb, err := engine.Open(localDB)
	if err != nil {
		t.Fatalf("open local db: %v", err)
	}
	defer ldb.Close()
	ldb.SetMaxOpenConns(1)

	// Verify indexed docs
	var docCount int
	if err := ldb.QueryRow(`SELECT COUNT(*) FROM _vec_emb_docs WHERE dataset_id = ?`, "rootA").Scan(&docCount); err != nil {
		t.Fatalf("count docs: %v", err)
	}
	if docCount == 0 {
		t.Fatalf("expected documents after indexing")
	}

	// Search should execute without error (uses simple embedder)
	fmt.Println("searchCmd start")
	searchCmd([]string{"--db", localDB, "--root", "rootA", "--query", "auth", "--embedder", "simple"})
	fmt.Println("searchCmd done")

	// Prepare upstream log DB
	upPath := filepath.Join(t.TempDir(), "upstream.sqlite")
	up, err := engine.Open(upPath)
	if err != nil {
		t.Fatalf("open upstream: %v", err)
	}
	defer up.Close()
	up.SetMaxOpenConns(1)
	if _, err := up.Exec(`CREATE TABLE vec_shadow_log (
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

	// Insert upstream add event
	payload := makePayload("rootA", "asset-x#0", "upstream content", "docs/u.txt", "deadbeef", 1, 0)
	if _, err := up.Exec(`INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload, created_at)
		VALUES(?,?,?,?,?,?,?)`, "rootA", "shadow_vec_docs", 1, "insert", "asset-x#0", payload, time.Now()); err != nil {
		t.Fatalf("insert log: %v", err)
	}

	fmt.Println("syncCmd start")
	syncCmd([]string{"--db", localDB, "--root", "rootA", "--upstream-driver", "sqlite", "--upstream-dsn", upPath})
	fmt.Println("syncCmd done")

	var upstreamCount int
	if err := ldb.QueryRow(`SELECT COUNT(*) FROM _vec_emb_docs WHERE dataset_id = ? AND id = ?`, "rootA", "asset-x#0").Scan(&upstreamCount); err != nil {
		t.Fatalf("count upstream doc: %v", err)
	}
	if upstreamCount != 1 {
		t.Fatalf("expected upstream doc after sync")
	}

	// Insert upstream delete event
	payloadDel := makePayload("rootA", "asset-x#0", "upstream content", "docs/u.txt", "deadbeef", 2, 1)
	if _, err := up.Exec(`INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload, created_at)
		VALUES(?,?,?,?,?,?,?)`, "rootA", "shadow_vec_docs", 2, "delete", "asset-x#0", payloadDel, time.Now()); err != nil {
		t.Fatalf("insert log delete: %v", err)
	}

	fmt.Println("syncCmd delete start")
	syncCmd([]string{"--db", localDB, "--root", "rootA", "--upstream-driver", "sqlite", "--upstream-dsn", upPath})
	fmt.Println("syncCmd delete done")

	var archived int
	if err := ldb.QueryRow(`SELECT archived FROM _vec_emb_docs WHERE dataset_id = ? AND id = ?`, "rootA", "asset-x#0").Scan(&archived); err != nil {
		t.Fatalf("archived check: %v", err)
	}
	if archived != 1 {
		t.Fatalf("expected archived after delete")
	}

	// Admin actions
	fmt.Println("adminCmd start")
	adminCmd([]string{"--db", localDB, "--root", "rootA", "--action", "rebuild"})
	adminCmd([]string{"--db", localDB, "--root", "rootA", "--action", "invalidate"})
	adminCmd([]string{"--db", localDB, "--root", "rootA", "--action", "check"})
	adminCmd([]string{"--db", localDB, "--root", "rootA", "--action", "prune"})
	fmt.Println("adminCmd done")

	// Archived doc should be pruned
	var remaining int
	if err := ldb.QueryRow(`SELECT COUNT(*) FROM _vec_emb_docs WHERE dataset_id = ? AND id = ?`, "rootA", "asset-x#0").Scan(&remaining); err != nil {
		t.Fatalf("post-prune count: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected pruned doc, got %d", remaining)
	}

	// Ensure the test exercised the sqlite driver for upstream.
	_ = ctx
}

func makePayload(datasetID, id, content, path, md5 string, scn int64, archived int) []byte {
	vecs := embedString(content, 64)
	blob, _ := vector.EncodeEmbedding(vecs)
	meta := map[string]interface{}{
		"asset_id": id,
		"path":     path,
		"md5":      md5,
	}
	metaJSON, _ := json.Marshal(meta)
	p := logPayload{
		DatasetID:      datasetID,
		ID:             id,
		Content:        content,
		Meta:           string(metaJSON),
		Embedding:      hex.EncodeToString(blob),
		EmbeddingModel: "simple",
		SCN:            scn,
		Archived:       archived,
	}
	b, _ := json.Marshal(&p)
	return b
}

func embedString(s string, dim int) []float32 {
	if dim <= 0 {
		dim = 64
	}
	v := make([]float32, dim)
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*16777619 ^ uint32(s[i])
	}
	seed := h
	for i := range v {
		seed = seed*1664525 + 1013904223
		v[i] = float32(seed%10000) / 10000.0
	}
	return v
}

func TestCLIFlow_RespectsIncludeExclude(t *testing.T) {
	rootDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootDir, "keep.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "skip.txt"), []byte("ignore me"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	localDB := filepath.Join(t.TempDir(), "embedius.sqlite")
	indexCmd([]string{"--db", localDB, "--root", "rootB", "--path", rootDir, "--embedder", "simple", "--include", "**/*.go"})

	ldb, err := engine.Open(localDB)
	if err != nil {
		t.Fatalf("open local db: %v", err)
	}
	defer ldb.Close()

	var cnt int
	if err := ldb.QueryRow(`SELECT COUNT(*) FROM _vec_emb_docs WHERE dataset_id = ?`, "rootB").Scan(&cnt); err != nil {
		t.Fatalf("count docs: %v", err)
	}
	if cnt == 0 {
		t.Fatalf("expected docs from include filter")
	}

	var txtCnt int
	if err := ldb.QueryRow(`SELECT COUNT(*) FROM _vec_emb_docs WHERE dataset_id = ? AND meta LIKE '%skip.txt%'`, "rootB").Scan(&txtCnt); err != nil {
		t.Fatalf("count skip.txt docs: %v", err)
	}
	if txtCnt != 0 {
		t.Fatalf("expected skip.txt to be excluded")
	}
}
