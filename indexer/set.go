package indexer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/embedius/vectordb"
	"strings"
)

// Set represents a collection of documents for a specific location
type Set struct {
	fs            afs.Service
	vectorDb      vectordb.VectorStore
	indexer       Indexer
	embedder      embeddings.Embedder
	namespace     string
	baseURL       string
	upstreamURL   string
	vectorOptions []vectorstores.Option
	cache         *cache.Map[string, document.Entry]
}

// SimilaritySearch performs similarity search against the vector vectorDb
func (s *Set) SimilaritySearch(ctx context.Context, query string, numDocuments int, opts ...vectorstores.Option) ([]schema.Document, error) {
	opts = s.ensureEmbedder(opts)
	return s.vectorDb.SimilaritySearch(ctx, query, numDocuments, opts...)
}

// ensureEmbedder ensures the vector vectorDb options include an embedder
func (s *Set) ensureEmbedder(opts []vectorstores.Option) []vectorstores.Option {
	return append(s.vectorOptions, opts...)
}

// Index indexes content at the specified URI
func (s *Set) Index(ctx context.Context, URI string) error {
	toAddDocuments, toRemove, err := s.indexer.Index(ctx, URI, s.cache)
	if err != nil {
		return fmt.Errorf("indexing failed: %w", err)
	}

	if len(toAddDocuments) == 0 && len(toRemove) == 0 {
		return nil // Nothing changed
	}

	// Remove documents that are no longer needed
	if len(toRemove) > 0 {
		for _, id := range toRemove {
			if err := s.vectorDb.Remove(ctx, id, s.vectorOptions...); err != nil {
				return fmt.Errorf("failed to remove document %s: %w", id, err)
			}
		}
	}

	// Add new or updated documents
	if len(toAddDocuments) > 0 {
		ids, err := s.vectorDb.AddDocuments(ctx, toAddDocuments, s.vectorOptions...)
		if err != nil {
			return fmt.Errorf("failed to add documents: %w", err)
		}
		// Update entries with new IDs
		if err = s.updateEntriesWithIDs(toAddDocuments, ids); err != nil {
			return fmt.Errorf("failed to update entries with IDs: %w", err)
		}
	}

	return s.Persist(ctx)
}

// updateEntriesWithIDs updates entries with IDs returned from the vector vectorDb
func (s *Set) updateEntriesWithIDs(documents []schema.Document, ids []string) error {
	for i, id := range ids {
		if i >= len(documents) || id == "" {
			continue
		}
		doc := documents[i]
		metadata := doc.Metadata
		docID, ok := metadata[document.DocumentID].(string)
		if !ok {
			fmt.Printf(document.DocumentID+" not found in metadata for document %+v\n", metadata)
			continue
		}

		entry, ok := s.cache.Get(docID)
		if !ok {
			return fmt.Errorf("entry not found for document ID %s", docID)
		}

		fragment := entry.Fragments[0]
		if len(entry.Fragments) > 1 { //more than one fragment use fragmentID
			fragmentID, ok := metadata[document.FragmentID].(string)
			if !ok {
				fmt.Printf(document.FragmentID+" not found in metadata for document %+v\n", metadata)
				continue
			}

			byID := entry.Fragments.ByFragmentID(docID)
			fragment = byID[fragmentID]
		}
		if fragment == nil {
			return fmt.Errorf("fragment not found for document ID %s", docID)
		}
		fragment.VectorDBID = id
		s.cache.Set(docID, entry)
	}
	return nil
}

// Persist saves the vector vectorDb and entry metadata
func (s *Set) Persist(ctx context.Context) error {
	// Persist vector vectorDb if supported
	persister, ok := s.vectorDb.(vectordb.Persister)
	if ok {
		if err := persister.Persist(ctx); err != nil {
			return fmt.Errorf("failed to persist vector vectorDb: %w", err)
		}
	}
	data, err := s.cache.Data()
	if err != nil {
		return fmt.Errorf("failed to marshal entries: %w", err)
	}
	URL := s.assetURL()
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(string(data)))
}

// assetURL returns the URL for storing the entries metadata
func (s *Set) assetURL() string {
	return url.Join(s.baseURL, fmt.Sprintf("cache_%s.json", s.namespace))
}

// assetURL returns the URL for storing the entries metadata
func (s *Set) upstreamAssetURL() string {
	return url.Join(s.upstreamURL, fmt.Sprintf("cache_%s.json", s.namespace))
}

// load loads entries metadata from storage
func (s *Set) load(ctx context.Context) error {
	if err := s.handleUpstreamSync(ctx); err != nil {
		return err
	}
	URL := s.assetURL()
	exists, err := s.fs.Exists(ctx, URL)
	if !exists {
		return nil
	}
	data, err := s.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return fmt.Errorf("failed to open asset: %w", err)
	}
	return s.cache.Load(data)
}

func (s *Set) handleUpstreamSync(ctx context.Context) error {
	if s.upstreamURL == "" {
		return nil
	}
	upstream, _ := s.fs.Object(ctx, s.upstreamAssetURL())
	if upstream == nil {
		return nil
	}
	downstream, _ := s.fs.Object(ctx, s.assetURL())
	if downstream == nil {
		return nil
	}

	if upstream.ModTime().Before(downstream.ModTime()) {
		return nil
	}

	var sourceURL = []string{
		upstream.URL(),
		strings.Replace(strings.Replace(upstream.URL(), ".json", ".dat", 1), "cache_", "index_", 1),
		strings.Replace(strings.Replace(upstream.URL(), ".json", ".tre", 1), "cache_", "index_", 1),
	}
	var destURL = []string{
		downstream.URL(),
		strings.Replace(strings.Replace(downstream.URL(), ".json", ".dat", 1), "cache_", "index_", 1),
		strings.Replace(strings.Replace(downstream.URL(), ".json", ".tre", 1), "cache_", "index_", 1),
	}

	var dataByURL = map[string][]byte{}
	for _, URL := range sourceURL {
		data, err := s.fs.DownloadWithURL(ctx, URL)
		if err != nil {
			return nil //we do not report error here, as it is not critical - we just do not have syncronization from upstream
		}
		dataByURL[URL] = data
	}
	for i, URL := range sourceURL {
		if err := s.fs.Upload(ctx, destURL[i], file.DefaultFileOsMode, bytes.NewReader(dataByURL[URL])); err != nil {
			return fmt.Errorf("failed to upload asset: %w", err) //this is critical as partial data might be uploaded
		}
	}
	return nil
}

// NewSet creates a new document set
func NewSet(ctx context.Context, baseURL string, store vectordb.VectorStore, indexer Indexer, namespace string, embedder embeddings.Embedder, opts ...Option) (*Set, error) {
	set := &Set{
		baseURL:   baseURL,
		fs:        afs.New(),
		indexer:   indexer,
		embedder:  embedder,
		cache:     cache.NewMap[string, document.Entry](),
		vectorDb:  store,
		namespace: namespace,
		vectorOptions: []vectorstores.Option{
			vectorstores.WithEmbedder(embedder),
			vectorstores.WithNameSpace(namespace),
		},
	}
	for _, opt := range opts {
		opt(set)
	}
	if err := set.load(ctx); err != nil {
		return nil, fmt.Errorf("failed to load set: %w", err)
	}
	return set, nil
}
