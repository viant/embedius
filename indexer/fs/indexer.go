package fs

import (
	"context"
	"fmt"
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/embedius/indexer/fs/matching"
	"github.com/viant/embedius/indexer/fs/splitter"
	"path/filepath"
	"strconv"
)

// Indexer implements indexing for filesystem resources
type Indexer struct {
	fs              afs.Service
	baseURL         string
	matcher         *matching.Manager
	splitterFactory *splitter.Factory
	embeddingsModel string
}

// New creates a new filesystem indexer
func New(baseURL string, embeddingsModel string, matcher *matching.Manager, splitterFactory *splitter.Factory) *Indexer {
	return &Indexer{
		fs:              afs.New(),
		baseURL:         baseURL,
		matcher:         matcher,
		embeddingsModel: embeddingsModel,
		splitterFactory: splitterFactory,
	}
}

// Namespace returns namespace
func (i *Indexer) Namespace(ctx context.Context, URI string) (string, error) {
	embeddingsHash, err := cache.Hash([]byte(i.embeddingsModel))
	if err != nil {
		return "", fmt.Errorf("failed to hash embedings %v: %v", URI, err)
	}

	uriHash, err := cache.Hash([]byte(URI))
	if err != nil {
		return "", fmt.Errorf("failed to hash URI %v: %v", URI, err)
	}
	return strconv.Itoa(int(embeddingsHash)) + "_" + strconv.Itoa(int(uriHash)), nil
}

// Index indexes content from the filesystem
func (i *Indexer) Index(ctx context.Context, location string, cache *cache.Map[string, document.Entry]) ([]schema.Document, []string, error) {
	path, err := filepath.Abs(location)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get absolute path for %s: %w", location, err)
	}
	objects, err := i.fs.List(ctx, path)
	if err != nil {
		return nil, nil, err
	}

	var toAddDocuments []schema.Document
	var toRemove []string

	for _, object := range objects {
		objectPath := url.Path(object.URL())
		if url.Equals(objectPath, location) {
			continue
		}

		if i.matcher.IsExcluded(object) {
			continue
		}

		name := object.Name()

		if object.IsDir() {
			// Recursively index subdirectories
			subDocuments, subToRemove, err := i.Index(ctx, url.Join(location, name), cache)
			if err != nil {
				return nil, nil, err
			}
			toAddDocuments = append(toAddDocuments, subDocuments...)
			toRemove = append(toRemove, subToRemove...)
			continue
		}

		docs, ids, err := i.indexFile(ctx, object, cache)
		if err != nil {
			return nil, nil, err
		}
		toAddDocuments = append(toAddDocuments, docs...)
		toRemove = append(toRemove, ids...)
	}

	return toAddDocuments, toRemove, nil
}

// indexFile indexes a single file
func (i *Indexer) indexFile(ctx context.Context, object storage.Object, cache *cache.Map[string, document.Entry]) ([]schema.Document, []string, error) {
	docId := url.Path(object.URL())
	data, err := i.fs.Download(ctx, object)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download %s: %w", docId, err)
	}

	dataHash, _ := computeHash(data)
	if dataHash == 0 {
		dataHash = uint64(object.ModTime().Unix())
	}

	prev, ok := cache.Get(docId)
	if ok {
		if prev.Hash == dataHash {
			return nil, nil, nil // No changes detected
		}
	}
	aSplitter := i.splitterFactory.GetSplitter(docId, len(data))
	// Create new entry

	entry := &document.Entry{
		ID:        docId,
		ModTime:   object.ModTime(),
		Hash:      dataHash,
		Fragments: aSplitter.Split(data, map[string]interface{}{document.DocumentID: docId}),
	}
	cache.Set(docId, entry)

	// Create documents from fragments
	var documents []schema.Document
	for _, fragment := range entry.Fragments {
		documents = append(documents, fragment.NewDocument(docId, data))
	}

	// Determine IDs to remove (from previous version)
	var toRemove []string
	if prev != nil {
		toRemove = prev.Fragments.VectorDBIDs()
	}
	return documents, toRemove, nil
}

// computeHash computes a hash for the given data
func computeHash(data []byte) (uint64, error) {
	// Implement appropriate hashing algorithm
	// For simplicity, using length as a placeholder
	return uint64(len(data)), nil
}
