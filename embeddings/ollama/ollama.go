package ollama

import (
	"context"
	"fmt"
)

type Embedder struct {
	C *Client
}

func NewClient(model, baseURL string) *Client {
	opts := []ClientOption{}
	if baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	return NewClientWithOptions(model, opts...)
}

func (e *Embedder) EmbedDocuments(ctx context.Context, docs []string) ([][]float32, error) {
	if e == nil || e.C == nil {
		return nil, fmt.Errorf("ollama embedder not configured")
	}
	vecs, _, err := e.C.Embed(ctx, docs)
	return vecs, err
}

func (e *Embedder) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	vecs, err := e.EmbedDocuments(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) != 1 {
		return nil, fmt.Errorf("embedder returned %d vectors for 1 query", len(vecs))
	}
	return vecs[0], nil
}
