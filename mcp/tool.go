package mcp

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	protoserver "github.com/viant/mcp-protocol/server"

	"github.com/viant/embedius/service"
)

//go:embed tools/search.md
var descSearch string

//go:embed tools/roots.md
var descRoots string

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
	model := h.model
	if in.Model != "" {
		model = in.Model
	}
	results, err := h.service.Search(ctx, service.SearchRequest{
		DBPath:   h.dbPath,
		Dataset:  in.Root,
		Query:    in.Query,
		Embedder: h.embedder,
		Model:    model,
		Limit:    limit,
		MinScore: in.MinScore,
	})
	if err != nil {
		return nil, err
	}
	return &SearchOutput{Results: results}, nil
}

func (h *Handler) roots(ctx context.Context, in *RootsInput) (*RootsOutput, error) {
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
	return &RootsOutput{Roots: infos}, nil
}
