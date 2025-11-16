package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	defaultBaseURL         = "https://api.openai.com/v1"
	embeddingsEndpoint     = "/embeddings"
	defaultEmbeddingModel  = "text-embedding-3-small"
	defaultHTTPClientTO    = 30 * time.Second
	defaultMaxEmbedRetries = 3
	defaultInitialBackoff  = 500 * time.Millisecond
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

// isRetryableStatus reports whether the given HTTP status code should be
// considered transient and retried by the Embed call.
func isRetryableStatus(code int) bool {
	if code == http.StatusTooManyRequests { // 429
		return true
	}
	if code >= 500 && code <= 599 {
		return true
	}
	return false
}

// Embed creates embeddings for the given texts
func (c *Client) Embed(ctx context.Context, texts []string) (vectors [][]float32, totalTokens int, err error) {
	req := AdaptRequest(texts, c.Model)
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}

	backoff := defaultInitialBackoff
	var lastErr error

	for attempt := 0; attempt <= defaultMaxEmbedRetries; attempt++ {
		if ctx.Err() != nil {
			// Context canceled or deadline exceeded; do not retry further.
			if lastErr != nil {
				return nil, 0, lastErr
			}
			return nil, 0, ctx.Err()
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+embeddingsEndpoint, bytes.NewReader(reqBody))
		if err != nil {
			return nil, 0, fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)

		resp, err := c.HTTPClient.Do(httpReq)
		if err != nil {
			// Do not retry on context-related errors.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				return nil, 0, fmt.Errorf("send request: %w", err)
			}
			lastErr = fmt.Errorf("send request: %w", err)
		} else {
			// We received a response; make sure the body is closed on all paths.
			func() {
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					// Retry on transient server errors and rate limiting.
					if isRetryableStatus(resp.StatusCode) && attempt < defaultMaxEmbedRetries {
						lastErr = fmt.Errorf("API error (status %d)", resp.StatusCode)
						return
					}
					var errResp struct {
						Error struct{ Message, Type string } `json:"error"`
					}
					_ = json.NewDecoder(resp.Body).Decode(&errResp)
					if errResp.Error.Message != "" {
						lastErr = fmt.Errorf("API error (%s): %s", errResp.Error.Type, errResp.Error.Message)
					} else {
						lastErr = fmt.Errorf("API error: %s", resp.Status)
					}
					return
				}

				data, err := io.ReadAll(resp.Body)
				if err != nil {
					lastErr = fmt.Errorf("read response: %w", err)
					return
				}
				var embeddingResp Response
				if err := json.Unmarshal(data, &embeddingResp); err != nil {
					lastErr = fmt.Errorf("decode response: %w", err)
					return
				}
				out := make([][]float32, len(embeddingResp.Data))
				for i := range embeddingResp.Data {
					out[i] = embeddingResp.Data[i].Embedding
				}
				vectors = out
				totalTokens = embeddingResp.Usage.TotalTokens
				lastErr = nil
			}()
		}

		if lastErr == nil {
			// Success.
			return vectors, totalTokens, nil
		}

		// If we've exhausted retries, return the last error.
		if attempt == defaultMaxEmbedRetries {
			return nil, 0, lastErr
		}

		// Wait with exponential backoff before the next attempt.
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	// This point should not be reached, but return a generic error as a safeguard.
	if lastErr == nil {
		lastErr = fmt.Errorf("embedding failed after %d attempts", defaultMaxEmbedRetries+1)
	}
	return nil, 0, lastErr
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
