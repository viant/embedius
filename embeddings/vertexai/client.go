package vertexai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	defaultLocation   = "us-central1"
	defaultModel      = "text-embedding-004"
	defaultHTTPTO     = 30 * time.Second
	defaultScopeCloud = "https://www.googleapis.com/auth/cloud-platform"
)

type ClientOption func(*Client)

func WithLocation(location string) ClientOption {
	return func(c *Client) {
		if location != "" {
			c.Location = location
		}
	}
}

func WithScopes(scopes ...string) ClientOption {
	return func(c *Client) {
		c.Scopes = append(c.Scopes, scopes...)
	}
}

func WithModel(model string) ClientOption {
	return func(c *Client) {
		if model != "" {
			c.Model = model
		}
	}
}

type Client struct {
	ProjectID string
	Location  string
	Model     string
	Scopes    []string

	httpClient  *http.Client
	tokenSource oauth2.TokenSource
}

type predictRequest struct {
	Instances []predictInstance `json:"instances"`
}

type predictInstance struct {
	Content string `json:"content"`
}

type predictResponse struct {
	Predictions []predictEmbedding `json:"predictions"`
}

type predictEmbedding struct {
	Embeddings predictEmbeddingValues `json:"embeddings"`
}

type predictEmbeddingValues struct {
	Values []float32 `json:"values"`
}

func NewClient(ctx context.Context, projectID, model string, opts ...ClientOption) (*Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("vertexai project id is required")
	}
	c := &Client{
		ProjectID:  projectID,
		Location:   defaultLocation,
		Model:      model,
		httpClient: &http.Client{Timeout: defaultHTTPTO},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.Model == "" {
		c.Model = defaultModel
	}
	if len(c.Scopes) == 0 {
		c.Scopes = []string{defaultScopeCloud}
	}
	ts, err := google.DefaultTokenSource(ctx, c.Scopes...)
	if err != nil {
		return nil, fmt.Errorf("vertexai token source: %w", err)
	}
	c.tokenSource = ts
	return c, nil
}

func (c *Client) endpoint() string {
	return fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		c.Location, c.ProjectID, c.Location, c.Model)
}

func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, int, error) {
	if c == nil {
		return nil, 0, fmt.Errorf("vertexai client is nil")
	}
	if len(texts) == 0 {
		return nil, 0, fmt.Errorf("no input texts provided")
	}
	instances := make([]predictInstance, 0, len(texts))
	for _, t := range texts {
		instances = append(instances, predictInstance{Content: t})
	}
	body, err := json.Marshal(predictRequest{Instances: instances})
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(), bytes.NewReader(body))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	token, err := c.tokenSource.Token()
	if err != nil {
		return nil, 0, fmt.Errorf("vertexai token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, 0, fmt.Errorf("send request: %w", err)
		}
		return nil, 0, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("vertexai API error: %s", strings.TrimSpace(string(body)))
	}
	var out predictResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, 0, fmt.Errorf("decode response: %w", err)
	}
	vecs := make([][]float32, 0, len(out.Predictions))
	for _, p := range out.Predictions {
		vecs = append(vecs, p.Embeddings.Values)
	}
	return vecs, 0, nil
}
