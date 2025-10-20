package retriever

import (
	"context"
	"github.com/viant/embedius/indexer"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"
)

// Service is a retriever service
type Service struct {
	indexer *indexer.Service
}

// Match retrieves documents matching the query
func (s *Service) Match(ctx context.Context, query string, limit int, location string, opts ...vectorstores.Option) ([]schema.Document, error) {

	set, err := s.indexer.Add(ctx, location)
	if err != nil {
		return nil, err
	}
	docs, err := set.SimilaritySearch(ctx, query, limit, opts...)

	return docs, err
}

func NewService(indexer *indexer.Service) *Service {
	return &Service{
		indexer: indexer,
	}
}
