package embeddings

import "context"

// Embedder is a minimal interface for computing vector embeddings
// for documents and queries.
type Embedder interface {
	EmbedDocuments(ctx context.Context, docs []string) ([][]float32, error)
	EmbedQuery(ctx context.Context, text string) ([]float32, error)
}
