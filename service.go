package retriever

import (
	"context"
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/embedius/indexer"
)

// Service is a retriever service
type Service struct {
	indexer *indexer.Service
}

// Match retrieves documents matching the query
func (s *Service) Match(ctx context.Context, query string, limit int, location string) ([]schema.Document, error) {
	set, err := s.indexer.Add(ctx, location)
	if err != nil {
		return nil, err
	}
	return set.SimilaritySearch(ctx, query, limit)
}

func NewService(indexer *indexer.Service) *Service {
	return &Service{
		indexer: indexer,
	}
}
