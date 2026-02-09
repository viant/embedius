package vertexai

import (
	"context"
	"fmt"
	"sync"
)

type Embedder struct {
	projectID string
	model     string
	location  string
	scopes    []string

	mu      sync.Mutex
	client  *Client
	initErr error
}

func NewEmbedder(projectID, model, location string, scopes []string) *Embedder {
	return &Embedder{
		projectID: projectID,
		model:     model,
		location:  location,
		scopes:    scopes,
	}
}

func (e *Embedder) EmbedDocuments(ctx context.Context, docs []string) ([][]float32, error) {
	client, err := e.getClient(ctx)
	if err != nil {
		return nil, err
	}
	vecs, _, err := client.Embed(ctx, docs)
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

func (e *Embedder) getClient(ctx context.Context) (*Client, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil || e.initErr != nil {
		return e.client, e.initErr
	}
	if e.projectID == "" {
		e.initErr = fmt.Errorf("vertexai project id is required")
		return nil, e.initErr
	}
	opts := []ClientOption{}
	if e.location != "" {
		opts = append(opts, WithLocation(e.location))
	}
	if len(e.scopes) > 0 {
		opts = append(opts, WithScopes(e.scopes...))
	}
	if e.model != "" {
		opts = append(opts, WithModel(e.model))
	}
	client, err := NewClient(ctx, e.projectID, e.model, opts...)
	if err != nil {
		e.initErr = err
		return nil, err
	}
	e.client = client
	return client, nil
}
