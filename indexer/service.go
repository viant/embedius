package indexer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/viant/afs"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/vectordb"
	"github.com/viant/embedius/vectordb/sqlitevec"
)

// Service manages document sets for different locations
type Service struct {
	baseURL   string
	fs        afs.Service
	sets      map[string]*Set // Sets mapped by location
	vStore    vectordb.VectorStore
	embedder  embeddings.Embedder
	mux       sync.RWMutex
	indexer   Indexer
	asyncMu   sync.Mutex
	inFlight  map[string]bool
	refreshed map[string]time.Time
}

// Embedder returns the embedder used by the service
func (s *Service) Embedder() embeddings.Embedder {
	return s.embedder
}

// Open creates or retrieves a set for the specified location without indexing
// its content. Callers use Open when indexing is managed independently, such as
// a foreground search paired with AddAsync.
func (s *Service) Open(ctx context.Context, location string) (*Set, error) {
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
	return set, nil
}

// Add creates or retrieves a set for the specified location and indexes its content.
func (s *Service) Add(ctx context.Context, location string) (*Set, error) {
	set, err := s.Open(ctx, location)
	if err != nil {
		return nil, err
	}
	if err = set.Index(ctx, location); err != nil {
		return nil, fmt.Errorf("failed to index content: %w", err)
	}
	return set, nil
}

// AddAsync triggers background indexing for the location if not already running.
func (s *Service) AddAsync(ctx context.Context, location string) {
	if strings.TrimSpace(location) == "" {
		return
	}
	s.asyncMu.Lock()
	if s.inFlight == nil {
		s.inFlight = map[string]bool{}
	}
	if s.inFlight[location] {
		s.asyncMu.Unlock()
		return
	}
	if interval := AsyncIndexRefreshInterval(ctx); interval > 0 {
		if refreshedAt := s.refreshed[location]; !refreshedAt.IsZero() && time.Since(refreshedAt) < interval {
			s.asyncMu.Unlock()
			return
		}
	}
	s.inFlight[location] = true
	s.asyncMu.Unlock()

	go func() {
		var err error
		defer func() {
			s.asyncMu.Lock()
			delete(s.inFlight, location)
			if err == nil {
				if s.refreshed == nil {
					s.refreshed = map[string]time.Time{}
				}
				s.refreshed[location] = time.Now()
			}
			s.asyncMu.Unlock()
		}()
		_, err = s.Add(ctx, location)
	}()
}

// NewService creates a new storage service
func NewService(baseURL string, vStore vectordb.VectorStore, embedder embeddings.Embedder, indexer Indexer) *Service {
	if vStore == nil {
		if baseURL == "" {
			panic("baseURL is required when vStore is nil")
		}
		dbPath := filepath.Join(baseURL, "embedius.sqlite")
		store, err := sqlitevec.NewStore(
			sqlitevec.WithDSN(dbPath),
			sqlitevec.WithEnsureSchema(true),
		)
		if err != nil {
			panic(fmt.Sprintf("failed to create sqlitevec store: %v", err))
		}
		store.SetSCNAllocator(sqlitevec.DefaultSCNAllocator(store.DB()))
		vStore = store
	}
	return &Service{
		vStore:   vStore,
		baseURL:  baseURL,
		embedder: embedder,
		indexer:  indexer,
		fs:       afs.New(),
		sets:     make(map[string]*Set),
	}
}
