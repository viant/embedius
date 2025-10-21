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
	// Ensure we don't exceed embedding input limits with very long queries by
	// enabling UTF-8 safe query splitting unless the caller already configured it.
	var o vectorstores.Options
	for _, fn := range opts {
		fn(&o)
	}
	effective := opts
	if o.MaxQueryBytes <= 0 {
		// Default: 4k window with 256B overlap; aggregator defaults to 'max'.
		effective = append(effective, vectorstores.WithQuerySplit(4096, 256, ""))
	}

	docs, err := set.SimilaritySearch(ctx, query, limit, effective...)

	return docs, err
}

func NewService(indexer *indexer.Service) *Service {
	return &Service{
		indexer: indexer,
	}
}
