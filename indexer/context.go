package indexer

import (
	"context"

	"github.com/viant/embedius/vectordb"
)

type upstreamSyncKey struct{}
type asyncIndexKey struct{}

// WithUpstreamSyncConfig attaches upstream sync config to the context.
func WithUpstreamSyncConfig(ctx context.Context, cfg *vectordb.UpstreamSyncConfig) context.Context {
	if ctx == nil || cfg == nil {
		return ctx
	}
	return context.WithValue(ctx, upstreamSyncKey{}, cfg)
}

func upstreamSyncConfig(ctx context.Context) *vectordb.UpstreamSyncConfig {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(upstreamSyncKey{}).(*vectordb.UpstreamSyncConfig); ok {
		return v
	}
	return nil
}

// WithAsyncIndex marks that indexing should run in background.
func WithAsyncIndex(ctx context.Context, enabled bool) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, asyncIndexKey{}, enabled)
}

// AsyncIndexEnabled reports whether async indexing is enabled.
func AsyncIndexEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if v, ok := ctx.Value(asyncIndexKey{}).(bool); ok {
		return v
	}
	return false
}

// UpstreamSyncConfigFromContext returns upstream config from context.
func UpstreamSyncConfigFromContext(ctx context.Context) *vectordb.UpstreamSyncConfig {
	return upstreamSyncConfig(ctx)
}
