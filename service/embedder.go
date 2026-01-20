package service

import "context"

// SimpleEmbedder returns deterministic vectors for local testing.
type SimpleEmbedder struct {
	Dim int
}

// NewSimpleEmbedder constructs a simple deterministic embedder.
func NewSimpleEmbedder(dim int) *SimpleEmbedder {
	if dim <= 0 {
		dim = 64
	}
	return &SimpleEmbedder{Dim: dim}
}

// EmbedDocuments embeds documents deterministically.
func (e *SimpleEmbedder) EmbedDocuments(ctx context.Context, docs []string) ([][]float32, error) {
	out := make([][]float32, len(docs))
	for i, s := range docs {
		out[i] = embedString(s, e.Dim)
	}
	return out, nil
}

// EmbedQuery embeds a query deterministically.
func (e *SimpleEmbedder) EmbedQuery(ctx context.Context, q string) ([]float32, error) {
	return embedString(q, e.Dim), nil
}

func embedString(s string, dim int) []float32 {
	if dim <= 0 {
		dim = 64
	}
	v := make([]float32, dim)
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*16777619 ^ uint32(s[i])
	}
	// Simple deterministic LCG-ish sequence based on hash.
	seed := h
	for i := range v {
		seed = seed*1664525 + 1013904223
		v[i] = float32(seed%10000) / 10000.0
	}
	return v
}
