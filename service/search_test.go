package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vector"
)

func TestSearchFallback(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(t.TempDir() + "/fallback.sqlite")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	// Open a connection before vec.Register runs to force a no-module path.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}

	if err := ensureSchema(ctx, conn); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	emb, _ := vector.EncodeEmbedding([]float32{0.2, 0.1, 0.3})
	metaJSON, _ := json.Marshal(map[string]interface{}{"path": "a.txt"})
	if _, err := conn.ExecContext(ctx, `INSERT INTO _vec_emb_docs(dataset_id, id, asset_id, content, meta, embedding, embedding_model, scn, archived)
VALUES(?,?,?,?,?,?,?,?,0)`, "rootA", "doc1", "asset1", "hello world", string(metaJSON), emb, "simple", 1); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}

	svc, err := NewService(WithDB(db), WithEmbedder(NewSimpleEmbedder(3)))
	if err != nil {
		t.Fatalf("service: %v", err)
	}

	results, err := svc.Search(ctx, SearchRequest{
		Dataset:  "rootA",
		Query:    "hello",
		Limit:    1,
		MinScore: 0,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "doc1" {
		t.Fatalf("expected doc1, got %s", results[0].ID)
	}
	if results[0].Path != "a.txt" {
		t.Fatalf("expected path a.txt, got %s", results[0].Path)
	}
}
