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
}

// WithEmbedder sets the embedder to use.
func WithEmbedder(e embeddings.Embedder) Option {
	return func(o *Options) { o.Embedder = e }
}

// WithNameSpace sets the logical namespace to operate on.
func WithNameSpace(ns string) Option {
	return func(o *Options) { o.NameSpace = ns }
}
