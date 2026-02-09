package ollama

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
)

const (
	defaultBaseURL     = "http://localhost:11434"
	embedEndpoint      = "/api/embed"
	defaultHTTPTimeout = 30 * time.Second
)

type ClientOption func(*Client)

func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) {
		if baseURL != "" {
			c.BaseURL = strings.TrimRight(baseURL, "/")
		}
	}
}

type Client struct {
	BaseURL    string
	Model      string
	HTTPClient *http.Client
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Embeddings      [][]float32 `json:"embeddings"`
	PromptEvalCount int         `json:"prompt_eval_count"`
	Error           string      `json:"error"`
}

func NewClientWithOptions(model string, opts ...ClientOption) *Client {
	c := &Client{
		BaseURL:    defaultBaseURL,
		Model:      model,
		HTTPClient: &http.Client{Timeout: defaultHTTPTimeout},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, int, error) {
	if c == nil {
		return nil, 0, fmt.Errorf("ollama client is nil")
	}
	if c.Model == "" {
		return nil, 0, fmt.Errorf("ollama model is required")
	}
	if len(texts) == 0 {
		return nil, 0, fmt.Errorf("no input texts provided")
	}
	reqBody, err := json.Marshal(embedRequest{Model: c.Model, Input: texts})
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+embedEndpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, 0, fmt.Errorf("send request: %w", err)
		}
		return nil, 0, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("ollama API error: %s", strings.TrimSpace(string(body)))
	}
	var out embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, 0, fmt.Errorf("decode response: %w", err)
	}
	if out.Error != "" {
		return nil, 0, fmt.Errorf("ollama API error: %s", out.Error)
	}
	return out.Embeddings, out.PromptEvalCount, nil
}
