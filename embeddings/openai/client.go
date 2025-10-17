package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	defaultBaseURL        = "https://api.openai.com/v1"
	embeddingsEndpoint    = "/embeddings"
	defaultEmbeddingModel = "text-embedding-3-small"
	defaultHTTPClientTO   = 30 * time.Second
)

// Request represents the request structure for OpenAI embeddings API
type Request struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

// Response represents the response structure from OpenAI embeddings API
type Response struct {
	Object string          `json:"object"`
	Data   []EmbeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  EmbeddingUsage  `json:"usage"`
}

// EmbeddingData represents a single embedding in the OpenAI embeddings API response
type EmbeddingData struct {
	Object    string    `json:"object"`
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

// EmbeddingUsage represents token usage information in the OpenAI embeddings API response
type EmbeddingUsage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type Client struct {
	BaseURL    string
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

func NewClient(apiKey, model string) *Client {
	c := &Client{
		BaseURL:    defaultBaseURL,
		APIKey:     apiKey,
		Model:      model,
		HTTPClient: &http.Client{Timeout: defaultHTTPClientTO},
	}
	if c.APIKey == "" {
		c.APIKey = os.Getenv("OPENAI_API_KEY")
	}
	if c.Model == "" {
		c.Model = defaultEmbeddingModel
	}
	return c
}

// AdaptRequest adapts texts to the OpenAI request payload.
func AdaptRequest(texts []string, model string) Request {
	return Request{Model: model, Input: texts}
}

// Embed creates embeddings for the given texts
func (c *Client) Embed(ctx context.Context, texts []string) (vectors [][]float32, totalTokens int, err error) {
	req := AdaptRequest(texts, c.Model)
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+embeddingsEndpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, 0, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Error struct{ Message, Type string } `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		if errResp.Error.Message != "" {
			return nil, 0, fmt.Errorf("API error (%s): %s", errResp.Error.Type, errResp.Error.Message)
		}
		return nil, 0, fmt.Errorf("API error: %s", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read response: %w", err)
	}
	var embeddingResp Response
	if err := json.Unmarshal(data, &embeddingResp); err != nil {
		return nil, 0, fmt.Errorf("decode response: %w", err)
	}
	out := make([][]float32, len(embeddingResp.Data))
	for i := range embeddingResp.Data {
		out[i] = embeddingResp.Data[i].Embedding
	}
	return out, embeddingResp.Usage.TotalTokens, nil
}

// Embedder bridges the client to the embeddings.Embedder interface.
type Embedder struct{ C *Client }

func (e *Embedder) EmbedDocuments(ctx context.Context, docs []string) ([][]float32, error) {
	v, _, err := e.C.Embed(ctx, docs)
	return v, err
}

func (e *Embedder) EmbedQuery(ctx context.Context, q string) ([]float32, error) {
	v, _, err := e.C.Embed(ctx, []string{q})
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return []float32{}, nil
	}
	return v[0], nil
}
