package vectorstores

import (
	"github.com/viant/embedius/embeddings"
)

// Option applies configuration to Options.
type Option func(*Options)

// Options collects optional parameters for vector store operations.
type Options struct {
	Embedder  embeddings.Embedder
	NameSpace string
	// Offset skips the first N results in ranked order.
	Offset int
	// Optional query-splitting configuration for long queries.
	// If MaxQueryBytes > 0 and the query exceeds this size, the query is split
	// into UTF-8 safe overlapping windows and embedded per-window.
	// Aggregation of per-window scores can be 'max' or 'mean' (default 'max').
	MaxQueryBytes   int
	QueryOverlap    int
	QueryAggregator string // "max" (default) or "mean"
}

// WithEmbedder sets the embedder to use.
func WithEmbedder(e embeddings.Embedder) Option {
	return func(o *Options) { o.Embedder = e }
}

// WithNameSpace sets the logical namespace to operate on.
func WithNameSpace(ns string) Option {
	return func(o *Options) { o.NameSpace = ns }
}

// WithOffset skips the first N results in ranked order.
func WithOffset(offset int) Option {
	return func(o *Options) {
		if offset > 0 {
			o.Offset = offset
		}
	}
}

// WithQuerySplit enables UTF-8 safe query splitting for long queries.
// maxBytes: maximum bytes per window (e.g., 4096). overlap: overlap in bytes between windows (e.g., 256).
// aggregator: "max" or "mean" to combine per-window scores. If empty, defaults to "max".
func WithQuerySplit(maxBytes, overlap int, aggregator string) Option {
	return func(o *Options) {
		if maxBytes > 0 {
			o.MaxQueryBytes = maxBytes
		}
		if overlap > 0 {
			o.QueryOverlap = overlap
		}
		if aggregator != "" {
			o.QueryAggregator = aggregator
		}
	}
}
