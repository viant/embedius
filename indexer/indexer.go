package indexer

import (
	"context"
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
)

// Indexer represents an interface for indexing content
type Indexer interface {
	// Index indexes content at the specified URI
	Index(ctx context.Context, URI string, cache *cache.Map[string, document.Entry]) (toAddDocuments []schema.Document, toRemove []string, err error)
	Namespace(ctx context.Context, URI string) (string, error)
}

// Splitter represents an interface for splitting content into fragments
type Splitter interface {
	// Split splits content into fragments
	Split(data []byte, metadata map[string]interface{}) []*document.Fragment
}
