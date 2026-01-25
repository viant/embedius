package indexer

import (
	"context"
	"testing"

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
	calls int
}

func (c *countingIndexer) Namespace(ctx context.Context, URI string) (string, error) {
	return "ns", nil
}

func (c *countingIndexer) Index(ctx context.Context, URI string, cache *cache.Map[string, document.Entry]) ([]schema.Document, []string, error) {
	c.calls++
	return nil, nil, nil
}

func TestServiceSkipIndexOnce(t *testing.T) {
	ctx := context.Background()
	idx := &countingIndexer{}
	svc := NewService("", noopStore{}, nil, idx)

	location := "file://example"
	svc.SkipIndexOnce(location)
	if _, err := svc.Add(ctx, location); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if idx.calls != 0 {
		t.Fatalf("expected indexer not called, got %d", idx.calls)
	}

	if _, err := svc.Add(ctx, location); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if idx.calls != 1 {
		t.Fatalf("expected indexer called once after skip consumed, got %d", idx.calls)
	}
}
