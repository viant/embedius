package indexer

import (
	"context"
	"fmt"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/viant/afs"
	"github.com/viant/embedius/vectordb"
	"sync"
)

// Service manages document sets for different locations
type Service struct {
	baseURL  string
	fs       afs.Service
	sets     map[string]*Set // Sets mapped by location
	vStore   vectordb.VectorStore
	embedder embeddings.Embedder
	mux      sync.RWMutex
	indexer  Indexer
}

// Embedder returns the embedder used by the service
func (s *Service) Embedder() embeddings.Embedder {
	return s.embedder
}

// Add creates or retrieves a set for the specified location and indexes its content
func (s *Service) Add(ctx context.Context, location string) (*Set, error) {
	namespace, err := s.indexer.Namespace(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("failed to get vector set URI: %w", err)
	}
	s.mux.Lock()
	set, exists := s.sets[namespace]
	if !exists {
		var err error
		set, err = NewSet(ctx, s.baseURL, s.vStore, s.indexer, namespace, s.embedder)
		if err != nil {
			s.mux.Unlock()
			return nil, fmt.Errorf("failed to create set: %w", err)
		}
		s.sets[namespace] = set
	}
	s.mux.Unlock()
	// Index the content
	if err = set.Index(ctx, location); err != nil {
		return nil, fmt.Errorf("failed to index content: %w", err)
	}
	return set, nil
}

// NewService creates a new storage service
func NewService(baseURL string, vStore vectordb.VectorStore, embedder embeddings.Embedder, indexer Indexer) *Service {
	return &Service{
		vStore:   vStore,
		baseURL:  baseURL,
		embedder: embedder,
		indexer:  indexer,
		fs:       afs.New(),
		sets:     make(map[string]*Set),
	}
}
