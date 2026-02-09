package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/viant/jsonrpc"
	streamingclient "github.com/viant/jsonrpc/transport/client/http/streamable"
	mcpschema "github.com/viant/mcp-protocol/schema"
	mcpclient "github.com/viant/mcp/client"

	emcp "github.com/viant/embedius/mcp"
	"github.com/viant/embedius/service"
)

type noopClientHandler struct{}

func (n *noopClientHandler) Implements(string) bool { return false }
func (n *noopClientHandler) Init(context.Context, *mcpschema.ClientCapabilities) {
}
func (n *noopClientHandler) OnNotification(context.Context, *jsonrpc.Notification) {}

func (n *noopClientHandler) Notify(context.Context, *jsonrpc.Notification) error { return nil }
func (n *noopClientHandler) NextRequestID() jsonrpc.RequestId {
	return jsonrpc.RequestId(1)
}
func (n *noopClientHandler) LastRequestID() jsonrpc.RequestId {
	return jsonrpc.RequestId(1)
}

func (n *noopClientHandler) ListRoots(context.Context, *jsonrpc.TypedRequest[*mcpschema.ListRootsRequest]) (*mcpschema.ListRootsResult, *jsonrpc.Error) {
	return nil, jsonrpc.NewMethodNotFound("not implemented", nil)
}
func (n *noopClientHandler) CreateMessage(context.Context, *jsonrpc.TypedRequest[*mcpschema.CreateMessageRequest]) (*mcpschema.CreateMessageResult, *jsonrpc.Error) {
	return nil, jsonrpc.NewMethodNotFound("not implemented", nil)
}
func (n *noopClientHandler) Elicit(context.Context, *jsonrpc.TypedRequest[*mcpschema.ElicitRequest]) (*mcpschema.ElicitResult, *jsonrpc.Error) {
	return nil, jsonrpc.NewMethodNotFound("not implemented", nil)
}

func mcpSearch(ctx context.Context, addr string, input *emcp.SearchInput) (*emcp.SearchOutput, error) {
	start := time.Now()
	cli, cleanup, err := newMCPClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	out, err := mcpSearchWithClient(ctx, cli, input)
	log.Printf("mcp metric op=search addr=%s root=%s dur=%s err=%v", addr, input.Root, time.Since(start), err)
	return out, err
}

func mcpRoots(ctx context.Context, addr string, input *emcp.RootsInput) (*emcp.RootsOutput, error) {
	start := time.Now()
	cli, cleanup, err := newMCPClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	out, err := mcpRootsWithClient(ctx, cli, input)
	log.Printf("mcp metric op=roots addr=%s dur=%s err=%v", addr, time.Since(start), err)
	return out, err
}

func mcpSearchAll(ctx context.Context, addr, query string, limit int, minScore float64, model string) ([]service.SearchResult, error) {
	start := time.Now()
	cli, cleanup, err := newMCPClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	out, err := mcpSearchAllWithClient(ctx, cli, query, limit, minScore, model)
	log.Printf("mcp metric op=search_all addr=%s dur=%s err=%v", addr, time.Since(start), err)
	return out, err
}

func mcpSearchAllWithClient(ctx context.Context, cli *mcpclient.Client, query string, limit int, minScore float64, model string) ([]service.SearchResult, error) {
	rootsOut, err := mcpRootsWithClient(ctx, cli, &emcp.RootsInput{})
	if err != nil {
		return nil, err
	}
	if len(rootsOut.Roots) == 0 {
		return nil, fmt.Errorf("mcp: no roots available")
	}
	if limit <= 0 {
		limit = 10
	}
	type rootResult struct {
		root    string
		results []service.SearchResult
		err     error
	}
	ch := make(chan rootResult, len(rootsOut.Roots))
	var wg sync.WaitGroup
	for _, info := range rootsOut.Roots {
		root := info.DatasetID
		if strings.TrimSpace(root) == "" {
			continue
		}
		wg.Add(1)
		go func(r string) {
			defer wg.Done()
			out, err := mcpSearchWithRetryClient(ctx, cli, &emcp.SearchInput{
				Root:     r,
				Query:    query,
				Limit:    limit,
				MinScore: minScore,
				Model:    model,
			})
			if err != nil {
				ch <- rootResult{root: r, err: err}
				return
			}
			ch <- rootResult{root: r, results: out.Results}
		}(root)
	}
	wg.Wait()
	close(ch)

	var merged []service.SearchResult
	for res := range ch {
		if res.err != nil {
			log.Printf("mcp search root=%s err=%v", res.root, res.err)
			continue
		}
		if len(res.results) == 0 {
			log.Printf("mcp search root=%s matched=0", res.root)
			continue
		}
		min, max := rootScoreRange(res.results)
		log.Printf("mcp search root=%s matched=%d score_min=%.4f score_max=%.4f", res.root, len(res.results), min, max)
		merged = append(merged, res.results...)
	}
	if len(merged) == 0 {
		return nil, nil
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Score == merged[j].Score {
			return merged[i].ID < merged[j].ID
		}
		return merged[i].Score > merged[j].Score
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, nil
}

func mcpSearchWithRetry(ctx context.Context, addr string, input *emcp.SearchInput) (*emcp.SearchOutput, error) {
	cli, cleanup, err := newMCPClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return mcpSearchWithRetryClient(ctx, cli, input)
}

func mcpSearchWithRetryClient(ctx context.Context, cli *mcpclient.Client, input *emcp.SearchInput) (*emcp.SearchOutput, error) {
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		out, err := mcpSearchWithClient(ctx, cli, input)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		msg := err.Error()
		if !isRetryableMCPError(msg) || attempt == maxAttempts {
			break
		}
		backoff := time.Duration(attempt*200) * time.Millisecond
		time.Sleep(backoff)
	}
	return nil, lastErr
}

func isRetryableMCPError(msg string) bool {
	msg = strings.ToLower(msg)
	return strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "temporarily unavailable") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "i/o timeout")
}

func newMCPClient(ctx context.Context, addr string) (*mcpclient.Client, func(), error) {
	url := normalizeMCPURL(addr)
	handler := mcpclient.NewHandler(&noopClientHandler{})
	transport, err := streamingclient.New(ctx, url, streamingclient.WithHandler(handler))
	if err != nil {
		return nil, nil, err
	}
	cli := mcpclient.New("embedius-cli", "0.1.0", transport)
	if _, err := cli.Initialize(ctx); err != nil {
		return nil, nil, err
	}
	return cli, func() { cli.Close() }, nil
}

func mcpSearchWithClient(ctx context.Context, cli *mcpclient.Client, input *emcp.SearchInput) (*emcp.SearchOutput, error) {
	params, err := mcpschema.NewCallToolRequestParams("search", input)
	if err != nil {
		return nil, err
	}
	res, err := cli.CallTool(ctx, params)
	if err != nil {
		return nil, err
	}
	if err := toolResultError(res); err != nil {
		return nil, err
	}
	var out emcp.SearchOutput
	if err := decodeToolResult(res, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func mcpRootsWithClient(ctx context.Context, cli *mcpclient.Client, input *emcp.RootsInput) (*emcp.RootsOutput, error) {
	params, err := mcpschema.NewCallToolRequestParams("roots", input)
	if err != nil {
		return nil, err
	}
	res, err := cli.CallTool(ctx, params)
	if err != nil {
		return nil, err
	}
	if err := toolResultError(res); err != nil {
		return nil, err
	}
	var out emcp.RootsOutput
	if err := decodeToolResult(res, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func normalizeMCPURL(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	if strings.HasSuffix(addr, "/mcp") {
		return addr
	}
	if strings.HasSuffix(addr, "/") {
		return addr + "mcp"
	}
	return addr + "/mcp"
}

func toolResultError(res *mcpschema.CallToolResult) error {
	if res == nil {
		return fmt.Errorf("mcp: empty response")
	}
	if res.IsError != nil && *res.IsError {
		return fmt.Errorf("mcp: %s", toolResultText(res))
	}
	return nil
}

func decodeToolResult(res *mcpschema.CallToolResult, out any) error {
	if res == nil {
		return fmt.Errorf("mcp: empty response")
	}
	if res.StructuredContent != nil {
		if v, ok := res.StructuredContent["result"]; ok {
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			return json.Unmarshal(b, out)
		}
	}
	text := toolResultText(res)
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("mcp: empty result")
	}
	return json.Unmarshal([]byte(text), out)
}

func toolResultText(res *mcpschema.CallToolResult) string {
	if res == nil {
		return ""
	}
	for _, elem := range res.Content {
		switch v := any(elem).(type) {
		case mcpschema.TextContent:
			if v.Text != "" {
				return v.Text
			}
		case *mcpschema.TextContent:
			if v != nil && v.Text != "" {
				return v.Text
			}
		case map[string]any:
			if t, ok := v["text"].(string); ok && t != "" {
				return t
			}
		default:
			if text := textFieldFromStruct(v); text != "" {
				return text
			}
		}
	}
	return ""
}

func rootScoreRange(results []service.SearchResult) (float64, float64) {
	min := results[0].Score
	max := results[0].Score
	for _, r := range results[1:] {
		if r.Score < min {
			min = r.Score
		}
		if r.Score > max {
			max = r.Score
		}
	}
	return min, max
}

func textFieldFromStruct(value any) string {
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	field := v.FieldByName("Text")
	if !field.IsValid() || field.Kind() != reflect.String {
		return ""
	}
	return field.String()
}
