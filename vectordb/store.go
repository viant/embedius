package vectordb

import (
	"context"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"
)

// VectorStore defines saving and querying documents using vector embeddings.
type VectorStore interface {
	AddDocuments(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) ([]string, error)
	SimilaritySearch(ctx context.Context, query string, numDocuments int, opts ...vectorstores.Option) ([]schema.Document, error)
	Remove(ctx context.Context, id string, option ...vectorstores.Option) error
}

type Persister interface {
	Persist(ctx context.Context) error
}
