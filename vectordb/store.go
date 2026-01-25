package vectordb

import (
	"context"
	"database/sql"
	"time"

	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"
)

// VectorStore defines saving and querying documents using vector embeddings.
type VectorStore interface {
	AddDocuments(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) ([]string, error)
	SimilaritySearch(ctx context.Context, query string, numDocuments int, opts ...vectorstores.Option) ([]schema.Document, error)
	Remove(ctx context.Context, id string, option ...vectorstores.Option) error
}

type Persister interface {
	Persist(ctx context.Context) error
}

// UpstreamSyncConfig controls optional upstream synchronization.
type UpstreamSyncConfig struct {
	Enabled     bool
	DatasetID   string
	UpstreamDB  *sql.DB
	Shadow      string
	BatchSize   int
	Force       bool
	Background  bool
	MinInterval time.Duration
	LocalShadow string
	AssetTable  string
	Filter      func(path, meta string) bool
	Logf        func(format string, args ...any)
}

// UpstreamSyncer runs an upstream sync when configured.
type UpstreamSyncer interface {
	SyncUpstream(ctx context.Context, cfg UpstreamSyncConfig) error
}
