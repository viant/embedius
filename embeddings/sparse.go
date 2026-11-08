package embeddings

import (
	"context"
)

// SparseVector maps lexical terms to weights.
type SparseVector map[string]float32

// SparseEmbedder computes lexical document and query embeddings. Scoring is a
// sparse dot product; it must not be passed to dense cosine-vector stores.
// A BM25 embedder is fitted to one corpus and replaced when that corpus changes.
type SparseEmbedder interface {
	EmbedDocuments(context.Context, []string) ([]SparseVector, error)
	EmbedQuery(context.Context, string) (SparseVector, error)
}
