package service

import (
	"context"
	"testing"

	"github.com/viant/sqlite-vec/engine"
)

func TestAdminCheckAndPrune(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(t.TempDir() + "/admin.sqlite")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()
	if err := ensureSchema(ctx, conn); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	if _, err := conn.ExecContext(ctx, `INSERT INTO emb_asset(dataset_id, asset_id, path, md5, size, mod_time, scn, archived)
VALUES(?,?,?,?,?,?,?,1)`, "rootA", "asset1", "a.txt", "deadbeef", 5, "2024-01-01T00:00:00Z", 2); err != nil {
		t.Fatalf("insert asset: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO _vec_emb_docs(dataset_id, id, asset_id, content, meta, embedding, embedding_model, scn, archived)
VALUES(?,?,?,?,?,?,?,?,1)`, "rootA", "doc1", "asset1", "archived", "{}", nil, "simple", 2); err != nil {
		t.Fatalf("insert doc: %v", err)
	}

	svc, err := NewService(WithDB(db))
	if err != nil {
		t.Fatalf("service: %v", err)
	}

	results, err := svc.Admin(ctx, AdminRequest{
		Roots:  []RootSpec{{Name: "rootA"}},
		Action: "check",
	})
	if err != nil {
		t.Fatalf("admin check: %v", err)
	}
	if len(results) != 1 || results[0].Stats == nil {
		t.Fatalf("expected stats result")
	}
	if results[0].Stats.Docs != 1 || results[0].Stats.Assets != 1 {
		t.Fatalf("unexpected stats: docs=%d assets=%d", results[0].Stats.Docs, results[0].Stats.Assets)
	}

	if _, err := svc.Admin(ctx, AdminRequest{
		Roots:    []RootSpec{{Name: "rootA"}},
		Action:   "prune",
		PruneSCN: 2,
		Force:    true,
	}); err != nil {
		t.Fatalf("admin prune: %v", err)
	}

	var remaining int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM _vec_emb_docs WHERE dataset_id = ?`, "rootA").Scan(&remaining); err != nil {
		t.Fatalf("count docs: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected pruned docs, got %d", remaining)
	}
}
