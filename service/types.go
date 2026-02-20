package service

import (
	"database/sql"

	"github.com/viant/embedius/embeddings"
)

// RootSpec defines a dataset root with optional filters.
type RootSpec struct {
	Name         string
	Path         string
	Include      []string
	Exclude      []string
	MaxSizeBytes int64
}

// ResolveRootsRequest specifies how roots should be resolved.
type ResolveRootsRequest struct {
	Root         string
	RootPath     string
	ConfigPath   string
	All          bool
	RequirePath  bool
	Include      []string
	Exclude      []string
	MaxSizeBytes int64
	SkipSecrets  bool
}

// IndexRequest defines inputs for indexing.
type IndexRequest struct {
	DBPath         string
	Roots          []RootSpec
	Embedder       embeddings.Embedder
	Model          string
	ChunkSize      int
	BatchSize      int
	Prune          bool
	Upstream       *sql.DB
	UpstreamDriver string
	UpstreamDSN    string
	UpstreamShadow string
	SyncBatch      int
	Logf           func(format string, args ...any)
	Progress       func(root string, current, total int, path string, tokens int)
}

// SyncRequest defines inputs for syncing upstream changes.
type SyncRequest struct {
	DBPath         string
	Roots          []RootSpec
	Upstream       *sql.DB
	UpstreamDriver string
	UpstreamDSN    string
	UpstreamShadow string
	SyncBatch      int
	Invalidate     bool
	ForceReset     bool
	Logf           func(format string, args ...any)
}

// PushRequest defines inputs for syncing local changes into a downstream log.
type PushRequest struct {
	DBPath           string
	Roots            []RootSpec
	Downstream       *sql.DB
	DownstreamDriver string
	DownstreamDSN    string
	DownstreamShadow string
	SyncBatch        int
	ApplyDownstream  bool
	DownstreamReset  bool
	Logf             func(format string, args ...any)
}

// SearchRequest defines inputs for embedding search.
type SearchRequest struct {
	DBPath   string
	Dataset  string
	Query    string
	Embedder embeddings.Embedder
	Model    string
	Limit    int
	Offset   int
	MinScore float64
}

// SearchResult describes a matched document.
type SearchResult struct {
	ID      string
	Score   float64
	Content string
	Meta    string
	Path    string
}

// RootsRequest defines inputs for root summary queries.
type RootsRequest struct {
	DBPath string
	Root   string
}

// RootInfo describes a root summary row.
type RootInfo struct {
	DatasetID      string
	SourceURI      string
	LastSCN        int64
	LastIndexedAt  sql.NullString
	Assets         int64
	AssetsArchived int64
	AssetsActive   int64
	AssetsSize     int64
	LastAssetMod   sql.NullString
	LastAssetMD5   sql.NullString
	Documents      int64
	DocsArchived   int64
	DocsActive     int64
	AvgDocLen      sql.NullFloat64
	LastDocSCN     sql.NullInt64
	EmbeddingModel sql.NullString
	LastSyncSCN    int64
	UpstreamShadow sql.NullString
}

// AdminRequest defines inputs for admin operations.
type AdminRequest struct {
	DBPath     string
	Roots      []RootSpec
	Action     string
	Shadow     string
	SyncShadow string
	PruneSCN   int64
	Force      bool
	Logf       func(format string, args ...any)
}

// AdminResult captures per-root admin outcomes.
type AdminResult struct {
	Root    string
	Action  string
	Details string
	Stats   *IntegrityStats
}

// IntegrityStats summarizes consistency checks.
type IntegrityStats struct {
	DatasetID         string
	Docs              int64
	DocsArchived      int64
	DocsActive        int64
	Assets            int64
	AssetsArchived    int64
	AssetsActive      int64
	OrphanDocs        int64
	OrphanAssets      int64
	MissingEmbeddings int64
}
