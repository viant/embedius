package sqlitevec

import (
	"context"
	"fmt"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type concurrencyEmbedder struct{}

func (concurrencyEmbedder) EmbedDocuments(ctx context.Context, docs []string) ([][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([][]float32, len(docs))
	for i := range out {
		out[i] = []float32{1, 0, 0, 0}
	}
	return out, nil
}
func (concurrencyEmbedder) EmbedQuery(ctx context.Context, _ string) ([]float32, error) {
	return []float32{1, 0, 0, 0}, ctx.Err()
}

func TestConcurrentStoreInstancesReadWriteNamespaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dbPath := filepath.Join(t.TempDir(), "capabilities.sqlite")
	stores := make([]*Store, 2)
	for i := range stores {
		s, err := NewStore(WithDSN(dbPath), WithWAL(true), WithBusyTimeout(2000))
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = s
		t.Cleanup(func() { _ = s.Close() })
	}
	start := make(chan struct{})
	errs := make(chan error, 12)
	var wg sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			ns := []string{"public-tools", "private-skills", "browser-tools"}[worker%3]
			options := []vectorstores.Option{vectorstores.WithNameSpace(ns), vectorstores.WithEmbedder(concurrencyEmbedder{})}
			for n := 0; n < 20; n++ {
				id := fmt.Sprintf("worker-%d-item-%d", worker, n)
				_, err := stores[worker%2].AddDocuments(ctx, []schema.Document{{PageContent: id, Metadata: map[string]interface{}{"fragmentId": id, "namespace": ns}}}, options...)
				if err != nil {
					errs <- fmt.Errorf("write: %w", err)
					return
				}
				rows, err := stores[(worker+1)%2].SimilaritySearch(ctx, "example", 3, options...)
				if err != nil {
					errs <- fmt.Errorf("search: %w", err)
					return
				}
				for _, row := range rows {
					if row.Metadata["namespace"] != ns {
						errs <- fmt.Errorf("namespace leakage")
						return
					}
				}
			}
		}(worker)
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent store operations did not finish before deadline")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var count int
	if err := stores[0].DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM _vec_emb_docs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 240 {
		t.Fatalf("persisted %d rows, want 240", count)
	}
	for _, ns := range []string{"public-tools", "private-skills", "browser-tools"} {
		for _, store := range stores {
			rows, err := store.SimilaritySearch(ctx, "example", 100, vectorstores.WithNameSpace(ns), vectorstores.WithEmbedder(concurrencyEmbedder{}))
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 80 {
				t.Fatalf("stale index for %s: got %d, want 80", ns, len(rows))
			}
		}
	}

	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := stores[0].AddDocuments(canceled, []schema.Document{{PageContent: "cancelled"}}, vectorstores.WithEmbedder(concurrencyEmbedder{})); err == nil {
		t.Fatal("cancelled write succeeded")
	}
}

func TestSearchDoesNotWriteDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := NewStore(WithDSN(filepath.Join(t.TempDir(), "readonly.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	opts := []vectorstores.Option{vectorstores.WithNameSpace("skills"), vectorstores.WithEmbedder(concurrencyEmbedder{})}
	if _, err := s.AddDocuments(ctx, []schema.Document{{PageContent: "example", Metadata: map[string]interface{}{"fragmentId": "example"}}}, opts...); err != nil {
		t.Fatal(err)
	}
	// One connection also detects nested acquisition deadlocks. Query-only
	// forbids schema changes, index persistence, and database lock records.
	s.DB().SetMaxOpenConns(1)
	if _, err := s.DB().ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	rows, err := s.SimilaritySearch(ctx, "example", 1, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows", len(rows))
	}
}
