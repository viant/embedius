package retriever

import (
	"context"
	"fmt"
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
	fmt.Printf("[embedius] retriever.Match: query=%q limit=%d location=%s\n", query, limit, location)
	set, err := s.indexer.Add(ctx, location)
	if err != nil {
		return nil, err
	}
	docs, err := set.SimilaritySearch(ctx, query, limit, opts...)
	if err == nil {
		fmt.Printf("[embedius] retriever.Match: returned %d docs\n", len(docs))
	}
	return docs, err
}

func NewService(indexer *indexer.Service) *Service {
	return &Service{
		indexer: indexer,
	}
}
