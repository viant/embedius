package mcp

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	protoserver "github.com/viant/mcp-protocol/server"

	"github.com/viant/embedius/service"
)

//go:embed tools/search.md
var descSearch string

//go:embed tools/roots.md
var descRoots string

//go:embed tools/list.md
var descList string

//go:embed tools/read.md
var descRead string

//go:embed tools/grepfiles.md
var descGrepFiles string

//go:embed tools/match.md
var descMatch string

func registerTools(registry *protoserver.Registry, h *Handler) error {
	if err := protoserver.RegisterTool[*SearchInput, *SearchOutput](registry, "search", descSearch, func(ctx context.Context, in *SearchInput) (*schema.CallToolResult, *jsonrpc.Error) {
		out, err := h.search(ctx, in)
		if err != nil {
			return buildErrorResult(err.Error())
		}
		return buildSuccessResult(out)
	}); err != nil {
		return err
	}

	if err := protoserver.RegisterTool[*RootsInput, *RootsOutput](registry, "roots", descRoots, func(ctx context.Context, in *RootsInput) (*schema.CallToolResult, *jsonrpc.Error) {
		out, err := h.roots(ctx, in)
		if err != nil {
			return buildErrorResult(err.Error())
		}
		return buildSuccessResult(out)
	}); err != nil {
		return err
	}

	if err := protoserver.RegisterTool[*ListInput, *ListOutput](registry, "list", descList, func(ctx context.Context, in *ListInput) (*schema.CallToolResult, *jsonrpc.Error) {
		out, err := h.list(ctx, in)
		if err != nil {
			return buildErrorResult(err.Error())
		}
		return buildSuccessResult(out)
	}); err != nil {
		return err
	}

	if err := protoserver.RegisterTool[*ReadInput, *ReadOutput](registry, "read", descRead, func(ctx context.Context, in *ReadInput) (*schema.CallToolResult, *jsonrpc.Error) {
		out, err := h.read(ctx, in)
		if err != nil {
			return buildErrorResult(err.Error())
		}
		return buildSuccessResult(out)
	}); err != nil {
		return err
	}

	if err := protoserver.RegisterTool[*GrepInput, *GrepOutput](registry, "grepFiles", descGrepFiles, func(ctx context.Context, in *GrepInput) (*schema.CallToolResult, *jsonrpc.Error) {
		out, err := h.grepFiles(ctx, in)
		if err != nil {
			return buildErrorResult(err.Error())
		}
		return buildSuccessResult(out)
	}); err != nil {
		return err
	}

	if err := protoserver.RegisterTool[*MatchInput, *MatchOutput](registry, "match", descMatch, func(ctx context.Context, in *MatchInput) (*schema.CallToolResult, *jsonrpc.Error) {
		out, err := h.match(ctx, in)
		if err != nil {
			return buildErrorResult(err.Error())
		}
		return buildSuccessResult(out)
	}); err != nil {
		return err
	}

	return nil
}

func buildErrorResult(message string) (*schema.CallToolResult, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.InvalidParams, message, nil)
}

func buildSuccessResult(payload any) (*schema.CallToolResult, *jsonrpc.Error) {
	b, _ := json.Marshal(payload)
	return &schema.CallToolResult{
		Content: []schema.CallToolResultContentElem{
			schema.TextContent{Type: "text", Text: string(b)},
		},
		StructuredContent: map[string]any{"result": payload},
	}, nil
}

func (h *Handler) search(ctx context.Context, in *SearchInput) (*SearchOutput, error) {
	start := time.Now()
	if h == nil || h.service == nil {
		return nil, fmt.Errorf("mcp: service unavailable")
	}
	if in == nil {
		in = &SearchInput{}
	}
	if in.Root == "" {
		return nil, fmt.Errorf("mcp: missing root")
	}
	if in.Query == "" {
		return nil, fmt.Errorf("mcp: missing query")
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 10
	}
	offset := in.Offset
	if offset < 0 {
		offset = 0
	}
	model := h.model
	if in.Model != "" {
		model = in.Model
	}
	qvec, cacheHit, err := h.queryEmbedding(ctx, in.Query, model)
	if err != nil {
		return nil, err
	}
	results, err := h.service.SearchWithEmbedding(ctx, service.SearchRequest{
		DBPath:   h.dbPath,
		Dataset:  in.Root,
		Query:    in.Query,
		Embedder: h.embedder,
		Model:    model,
		Limit:    limit,
		Offset:   offset,
		MinScore: in.MinScore,
	}, qvec)
	if err != nil {
		return nil, err
	}
	if h.metricsLog {
		log.Printf("mcp metric op=search root=%s matches=%d dur=%s cache_hit=%t", in.Root, len(results), time.Since(start), cacheHit)
	}
	return &SearchOutput{Results: results}, nil
}

func (h *Handler) roots(ctx context.Context, in *RootsInput) (*RootsOutput, error) {
	start := time.Now()
	if h == nil || h.service == nil {
		return nil, fmt.Errorf("mcp: service unavailable")
	}
	if in == nil {
		in = &RootsInput{}
	}
	infos, err := h.service.Roots(ctx, service.RootsRequest{DBPath: h.dbPath, Root: in.Root})
	if err != nil {
		return nil, err
	}
	if h.metricsLog {
		log.Printf("mcp metric op=roots count=%d dur=%s", len(infos), time.Since(start))
	}
	return &RootsOutput{Roots: infos}, nil
}
