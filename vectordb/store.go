package vectordb

import (
	"context"
	"github.com/tmc/langchaingo/vectorstores"
)

// VectorStore is the interface for saving and querying documents in the
// form of vector embeddings.
type VectorStore interface {
	vectorstores.VectorStore
	Remove(ctx context.Context, id string, option ...vectorstores.Option) error
}

type Persister interface {
	Persist(ctx context.Context) error
}
