package indexer

import (
	"context"
	"time"

	"github.com/viant/embedius/vectordb"
)

type upstreamSyncKey struct{}
type asyncIndexKey struct{}
type asyncIndexRefreshIntervalKey struct{}

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

// WithAsyncIndexRefreshInterval limits how frequently a location may complete
// background indexing. A non-positive interval preserves the existing behavior
// of checking on every request.
func WithAsyncIndexRefreshInterval(ctx context.Context, interval time.Duration) context.Context {
	if ctx == nil || interval <= 0 {
		return ctx
	}
	return context.WithValue(ctx, asyncIndexRefreshIntervalKey{}, interval)
}

// AsyncIndexRefreshInterval returns the configured background refresh interval.
func AsyncIndexRefreshInterval(ctx context.Context) time.Duration {
	if ctx == nil {
		return 0
	}
	if value, ok := ctx.Value(asyncIndexRefreshIntervalKey{}).(time.Duration); ok && value > 0 {
		return value
	}
	return 0
}

// UpstreamSyncConfigFromContext returns upstream config from context.
func UpstreamSyncConfigFromContext(ctx context.Context) *vectordb.UpstreamSyncConfig {
	return upstreamSyncConfig(ctx)
}
