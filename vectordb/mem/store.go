package mem

import (
	"context"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"sync"
)

const defaultSetID = "default"

type Store struct {
	baseURL string
	sets    map[string]*Set
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
		if set, err = NewSet(ctx, s.baseURL, setName); err != nil {
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
