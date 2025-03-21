package mem

import (
	"bytes"
	"context"
	"fmt"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/matching"
	"github.com/viant/embedius/matching/option"
	store "github.com/viant/embedius/vectordb"
	"github.com/viant/embedius/vectordb/meta"
	"github.com/viant/gds/tree/cover"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Set struct {
	id    string
	set   *cover.Tree[*store.Document]
	cache *MRUCache
	sync.RWMutex
	baseURL string
	fs      afs.Service
}

func (s *Set) getBaseURL() string {
	if s.baseURL != "" {
		return s.baseURL
	}
	return url.Join(os.Getenv("HOME"), ".gds")
}

func (s *Set) load(ctx context.Context) error {
	fs := afs.New()
	treeURL := s.assetURL(".tre")
	dataURL := s.assetURL(".dat")
	if ok, _ := fs.Exists(ctx, treeURL); !ok {
		return nil
	}
	treeReader, err := fs.OpenURL(ctx, treeURL)
	if err != nil {
		return err
	}
	defer treeReader.Close()
	if err := s.set.DecodeTree(treeReader); err != nil {
		return err
	}
	dataReader, err := fs.OpenURL(ctx, dataURL)
	if err != nil {
		return err
	}
	defer dataReader.Close()
	if err := s.set.DecodeValues(dataReader); err != nil {
		return err
	}
	return nil
}

func (s *Set) persist(ctx context.Context) error {
	fs := afs.New()
	treeWriter := new(bytes.Buffer)
	dataWriter := new(bytes.Buffer)
	if err := s.set.EncodeTree(treeWriter); err != nil {
		return err
	}
	if err := s.set.EncodeValues(dataWriter); err != nil {
		return err
	}

	treeURL := s.assetURL(".tre")
	if ok, _ := fs.Exists(ctx, treeURL); ok {
		_ = fs.Delete(ctx, treeURL)
	}
	if err := fs.Upload(ctx, treeURL, file.DefaultFileOsMode, treeWriter); err != nil {
		return err
	}
	dataURL := s.assetURL(".dat")
	if ok, _ := fs.Exists(ctx, dataURL); ok {
		_ = fs.Delete(ctx, dataURL)
	}
	if err := fs.Upload(ctx, dataURL, file.DefaultFileOsMode, dataWriter); err != nil {
		return err
	}
	return nil
}

func (s *Set) assetURL(ext string) string {
	builder := strings.Builder{}
	builder.WriteString("index")
	builder.WriteString("_")
	builder.WriteString(s.id)
	builder.WriteString(ext)
	treeURL := url.Join(s.getBaseURL(), builder.String())
	return treeURL
}

func (s *Set) AddDocuments(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) ([]string, error) {
	// Configure options.
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}

	concurrencyLimit := 16
	limiter := make(chan struct{}, concurrencyLimit)
	var wg sync.WaitGroup

	failedCount := uint32(0)

	counter := uint32(0)
	// Create a slice to store results per document (to preserve order).
	docIDs := make([][]string, len(docs))

	var err error
	// Process each document concurrently.
	for i, doc := range docs {
		// Check context cancellation.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		wg.Add(1)
		limiter <- struct{}{} // Acquire a token.
		// Capture the loop variables.
		go func(index int, document schema.Document) {
			defer wg.Done()
			defer func() { <-limiter }() // Release the token when done.

			if err != nil {
				return
			}
			var tempIDs []string

			// Convert to the proper document type.
			docStore := store.Document(document)

			// Get the document content.
			content, vErr := docStore.Content()
			if vErr != nil {
				err = vErr
				return
			}
			var vectors [][]float32
			for i := 0; i < 3; i++ {
				// Embed the document content.
				vectors, vErr = options.Embedder.EmbedDocuments(ctx, content)
				if vErr != nil {
					err = vErr
					if strings.Contains(strings.ToLower(vErr.Error()), "timeout") {
						continue
					}
					atomic.AddUint32(&failedCount, 1)
					fmt.Printf("failed to embed document %+v: %v", document, vErr)
					return
				} //no
				break
			}

			// Process each vector.
			for _, vector := range vectors {
				point := cover.NewPoint(vector...)
				s.RWMutex.Lock()
				if atomic.AddUint32(&counter, 1)%100 == 0 {
					fmt.Printf("processed %d of %v documents\n", counter, len(docs))
				}
				idx := s.set.Insert(&docStore, point)
				s.RWMutex.Unlock()
				tempIDs = append(tempIDs, fmt.Sprintf("%v", idx))
			}

			// Store the result in the correct order.
			docIDs[index] = tempIDs
		}(i, doc)
	}

	// Wait for all goroutines to complete.
	wg.Wait()

	if err != nil {
		return nil, err
	}
	fmt.Printf("processed %d of %v documents, failed: %v\n", counter, len(docs), failedCount)
	// Flatten the results while preserving the order of documents.
	var ids []string
	for _, slice := range docIDs {
		ids = append(ids, strings.Join(slice, "|"))
	}
	return ids, err
}

func (s *Set) SimilaritySearch(ctx context.Context, query string, numDocuments int, opts ...vectorstores.Option) ([]schema.Document, error) {
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	var matcher *matching.Manager

	if options.Filters != nil {
		if matchOptions, ok := options.Filters.([]option.Option); ok {
			matcher = matching.New(matchOptions...)
		}
	}
	// Check cache first
	if cachedVector, found := s.cache.Get(query); found {
		return s.searchWithVector(cachedVector, numDocuments, matcher), nil
	}

	vectors, err := options.Embedder.EmbedDocuments(ctx, []string{query})
	if err != nil {
		return nil, err
	}
	point := cover.NewPoint(vectors[0]...)
	// Cache the vector
	s.cache.Put(query, point)
	docs := s.searchWithVector(point, numDocuments, matcher)
	return docs, nil
}

func (s *Set) searchWithVector(point *cover.Point, numDocuments int, matcher *matching.Manager) []schema.Document {
	var docs = make([]schema.Document, 0)
	unique := make(map[string]bool)
	neighbors := s.set.KNearestNeighbors(point, numDocuments)

	for _, neighbor := range neighbors {
		if len(docs) >= numDocuments {
			return docs
		}
		doc := s.set.Value(neighbor.Point)
		if numDocuments > 1 {
			if _, ok := unique[doc.PageContent]; ok {
				continue
			}
			unique[doc.PageContent] = true
		}
		if matcher != nil {
			if s.IsExcluded(doc, matcher) {
				continue
			}
		}

		ret := *doc
		ret.Score = 1 - neighbor.Distance

		docs = append(docs, schema.Document(ret))
	}
	return docs
}

func (s *Set) Remove(ctx context.Context, id string, opts ...vectorstores.Option) error {
	for _, key := range strings.Split(id, "|") {
		idx, err := strconv.Atoi(key)
		if err != nil {
			return err
		}
		if point := s.set.FindPointByIndex(int32(idx)); point != nil {
			s.set.Remove(point)
		}
	}
	return nil
}

func (s *Set) IsExcluded(doc *store.Document, matcher *matching.Manager) bool {
	path := meta.GetString(doc.Metadata, "path")
	if path == "" {
		return false
	}
	return matcher.IsExcluded(path, len(doc.PageContent))
}

func NewSet(ctx context.Context, baseURL string, id string) (*Set, error) {
	set := &Set{
		id:      id,
		fs:      afs.New(),
		baseURL: baseURL,
		set:     cover.NewTree[*store.Document](1.3, cover.DistanceFunctionCosine),
		cache:   NewMRUCache(100),
	}
	return set, set.load(ctx)
}
