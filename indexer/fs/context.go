package fs

import "context"

type md5ContextKey struct{}
type indexRootKey struct{}
type assetMetaKey struct{}
type indexBaseKey struct{}
type indexStatsKey struct{}

// IndexStats tracks indexing progress for a root scan.
type IndexStats struct {
	Total     int
	Processed int
	Unchanged int
	Changed   int
	Docs      int
}

// WithExistingMD5s attaches a map of known md5 hashes to the context.
func WithExistingMD5s(ctx context.Context, md5s map[string]bool) context.Context {
	if ctx == nil || md5s == nil {
		return ctx
	}
	return context.WithValue(ctx, md5ContextKey{}, md5s)
}

func existingMD5s(ctx context.Context) map[string]bool {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(md5ContextKey{}).(map[string]bool); ok {
		return v
	}
	return nil
}

// WithIndexRoot marks the root location to report progress for.
func WithIndexRoot(ctx context.Context, root string) context.Context {
	if ctx == nil || root == "" {
		return ctx
	}
	return context.WithValue(ctx, indexRootKey{}, root)
}

func indexRoot(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(indexRootKey{}).(string); ok {
		return v
	}
	return ""
}

// AssetMeta captures upstream asset state keyed by path.
type AssetMeta struct {
	Size int64
	MD5  string
}

// WithExistingAssets attaches upstream asset metadata by path.
func WithExistingAssets(ctx context.Context, assets map[string]AssetMeta) context.Context {
	if ctx == nil || assets == nil {
		return ctx
	}
	return context.WithValue(ctx, assetMetaKey{}, assets)
}

func existingAssets(ctx context.Context) map[string]AssetMeta {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(assetMetaKey{}).(map[string]AssetMeta); ok {
		return v
	}
	return nil
}

// WithIndexBase stores the normalized base location for relative path extraction.
func WithIndexBase(ctx context.Context, base string) context.Context {
	if ctx == nil || base == "" {
		return ctx
	}
	return context.WithValue(ctx, indexBaseKey{}, base)
}

func indexBase(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(indexBaseKey{}).(string); ok {
		return v
	}
	return ""
}

// WithIndexStats attaches a shared stats tracker to the context.
func WithIndexStats(ctx context.Context, stats *IndexStats) context.Context {
	if ctx == nil || stats == nil {
		return ctx
	}
	return context.WithValue(ctx, indexStatsKey{}, stats)
}

func indexStats(ctx context.Context) *IndexStats {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(indexStatsKey{}).(*IndexStats); ok {
		return v
	}
	return nil
}
