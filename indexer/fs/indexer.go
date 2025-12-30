package fs

import (
	"context"
	"fmt"
	neturl "net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/embedius/indexer/fs/splitter"
	"github.com/viant/embedius/matching"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectordb/meta"
)

// Indexer implements indexing for filesystem resources
type Indexer struct {
	fs              Service
	baseURL         string
	matcher         *matching.Manager
	splitterFactory *splitter.Factory
	embeddingsModel string
}

// New creates a new filesystem indexer
func New(baseURL string, embeddingsModel string, matcher *matching.Manager, splitterFactory *splitter.Factory) *Indexer {
	return &Indexer{
		fs:              NewAFS(),
		baseURL:         baseURL,
		matcher:         matcher,
		embeddingsModel: embeddingsModel,
		splitterFactory: splitterFactory,
	}
}

// NewWithFS creates a new filesystem indexer with a custom FS service implementation.
func NewWithFS(baseURL string, embeddingsModel string, matcher *matching.Manager, splitterFactory *splitter.Factory, fsSvc Service) *Indexer {
	if fsSvc == nil {
		fsSvc = NewAFS()
	}
	return &Indexer{
		fs:              fsSvc,
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
	// Optional MCP server prefix for readability when URI is mcp: or mcp://
	prefix := ""
	if strings.HasPrefix(URI, "mcp://") {
		if u, err := neturl.Parse(URI); err == nil {
			if host := strings.TrimSpace(u.Host); host != "" {
				prefix = "mcp_" + sanitize(host) + "_"
			}
		}
	} else if strings.HasPrefix(URI, "mcp:") {
		raw := strings.TrimPrefix(URI, "mcp:")
		server := raw
		if i := strings.IndexByte(raw, ':'); i != -1 {
			server = raw[:i]
		} else if j := strings.IndexByte(raw, '/'); j != -1 {
			server = raw[:j]
		}
		server = strings.TrimSpace(server)
		if server != "" {
			prefix = "mcp_" + sanitize(server) + "_"
		}
	}
	// Avoid negative numbers by using unsigned formatting
	return prefix + strconv.FormatUint(embeddingsHash, 10) + "_" + strconv.FormatUint(uriHash, 10), nil
}

// sanitize converts server names to a filesystem-friendly token
func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			out = append(out, r)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

// Index indexes content from the filesystem
func (i *Indexer) Index(ctx context.Context, location string, cache *cache.Map[string, document.Entry]) ([]schema.Document, []string, error) {
	// Normalize the incoming location for cross-platform AFS compatibility.
	// - If relative with no scheme → make absolute OS path
	// - If absolute OS path with no scheme (drive/UNC/POSIX) → convert to file:// URL
	norm := location
	if url.Scheme(norm, "") == "" && url.IsRelative(norm) {
		var err error
		norm, err = filepath.Abs(norm)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get absolute path for %s: %w", location, err)
		}
	}
	if url.Scheme(norm, "") == "" && !url.IsRelative(norm) {
		norm = url.ToFileURL(norm)
	}

	objects, err := i.fs.List(ctx, norm)
	if err != nil {
		return nil, nil, err
	}

	var toAddDocuments []schema.Document
	var toRemove []string

	baseNormalised := norm
	if len(objects) > 0 {
		scheme := url.SchemeExtensionURL(objects[0].URL())
		baseNormalised = url.Normalize(norm, scheme)
	}

	for _, object := range objects {
		objectPath := url.Path(object.URL())
		if url.Equals(objectPath, location) && object.IsDir() {
			continue
		}
		if i.matcher.IsExcluded(url.Path(object.URL()), int(object.Size())) {
			continue
		}
		name := object.Name()

		if object.IsDir() {
			oUrl := object.URL()
			if baseNormalised == oUrl {
				continue
			}

			// Recursively index subdirectories
			subDocuments, subToRemove, err := i.Index(ctx, url.Join(norm, name), cache)
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
		ID:      docId,
		ModTime: object.ModTime(),
		Hash:    dataHash,
		Fragments: aSplitter.Split(data, map[string]interface{}{
			meta.DocumentID: docId,
			"path":          docId,
		}),
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
