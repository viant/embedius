package mem

import (
	"context"
	"fmt"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"
	"sync"
	"time"
)

const defaultSetID = "default"

type Store struct {
	baseURL        string
	sets           map[string]*Set
	setOptions     []SetOption
	writerLeaseTTL time.Duration
	writerMetrics  WriterMetrics
	sync.RWMutex
}

func (s *Store) BaseURL() string {
	return s.baseURL
}

func (s *Store) Persist(ctx context.Context) error {
	s.RWMutex.RLock()
	sets := s.sets
	s.RWMutex.RUnlock()
	for _, set := range sets {
		if err := set.persist(ctx); err != nil {
			return err
		}
		// Ensure value store data/manifest are flushed
		if set.values != nil {
			if err := set.values.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) AddDocuments(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) ([]string, error) {
	set, err := s.getSet(ctx, opts)
	if err != nil {
		return nil, err
	}
	return set.AddDocuments(ctx, docs, opts...)
}

func (s *Store) SimilaritySearch(ctx context.Context, query string, numDocuments int, opts ...vectorstores.Option) ([]schema.Document, error) {
	set, err := s.getSet(ctx, opts)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[embedius] mem.Store.SimilaritySearch: baseURL=%s set=%s query=%q k=%d\n", s.baseURL, s.getSetName(opts), query, numDocuments)
	return set.SimilaritySearch(ctx, query, numDocuments, opts...)
}

func (s *Store) Remove(ctx context.Context, id string, opts ...vectorstores.Option) error {
	set, err := s.getSet(ctx, opts)
	if err != nil {
		return err
	}
	if err = set.Remove(ctx, id, opts...); err != nil {
		return err
	}
	return nil
}

func (s *Store) getSet(ctx context.Context, opts []vectorstores.Option) (*Set, error) {
	setName := s.getSetName(opts)
	var err error
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	set, ok := s.sets[setName]
	if !ok {
		if set, err = NewSet(ctx, s.baseURL, setName, s.setOptions...); err != nil {
			return nil, err
		}
		s.sets[setName] = set
	}
	return set, nil
}

func (s *Store) getSetName(opts []vectorstores.Option) string {
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	set := options.NameSpace
	if set == "" {
		set = defaultSetID
	}
	return set
}

func NewStore(options ...StoreOption) *Store {
	ret := &Store{
		sets: make(map[string]*Set),
	}
	for _, opt := range options {
		opt(ret)
	}
	return ret
}

// Close closes all underlying ValueStores.
func (s *Store) Close() error {
	s.RWMutex.RLock()
	sets := make([]*Set, 0, len(s.sets))
	for _, st := range s.sets {
		sets = append(sets, st)
	}
	s.RWMutex.RUnlock()
	var firstErr error
	for _, set := range sets {
		if err := set.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
