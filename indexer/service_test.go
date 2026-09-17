package indexer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"
)

type noopStore struct{}

func (n noopStore) AddDocuments(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) ([]string, error) {
	return nil, nil
}

func (n noopStore) SimilaritySearch(ctx context.Context, query string, numDocuments int, opts ...vectorstores.Option) ([]schema.Document, error) {
	return nil, nil
}

func (n noopStore) Remove(ctx context.Context, id string, option ...vectorstores.Option) error {
	return nil
}

type countingIndexer struct {
	calls atomic.Int32
}

func (c *countingIndexer) Namespace(ctx context.Context, URI string) (string, error) {
	return "ns", nil
}

func (c *countingIndexer) Index(ctx context.Context, URI string, cache *cache.Map[string, document.Entry]) ([]schema.Document, []string, error) {
	c.calls.Add(1)
	return nil, nil, nil
}

func TestServiceOpenDoesNotIndex(t *testing.T) {
	ctx := context.Background()
	idx := &countingIndexer{}
	svc := NewService("", noopStore{}, nil, idx)

	location := "file://example"
	if _, err := svc.Open(ctx, location); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if idx.calls.Load() != 0 {
		t.Fatalf("expected indexer not called, got %d", idx.calls.Load())
	}

	if _, err := svc.Add(ctx, location); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if idx.calls.Load() != 1 {
		t.Fatalf("expected indexer called once after Open, got %d", idx.calls.Load())
	}
}

func TestServiceAddAsyncHonorsRefreshInterval(t *testing.T) {
	ctx := WithAsyncIndexRefreshInterval(context.Background(), time.Hour)
	idx := &countingIndexer{}
	svc := NewService("", noopStore{}, nil, idx)
	location := "file://example"

	svc.AddAsync(ctx, location)
	deadline := time.Now().Add(time.Second)
	for {
		svc.asyncMu.Lock()
		refreshed := !svc.refreshed[location].IsZero()
		svc.asyncMu.Unlock()
		if refreshed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background refresh did not complete")
		}
		time.Sleep(time.Millisecond)
	}

	svc.AddAsync(ctx, location)
	time.Sleep(20 * time.Millisecond)
	if actual := idx.calls.Load(); actual != 1 {
		t.Fatalf("expected one index scan within refresh interval, got %d", actual)
	}
}
