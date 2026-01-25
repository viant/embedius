package fs

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
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

	root := indexRoot(ctx)
	logProgress := root != "" && root == location
	processed := 0
	nextLog := 1000
	totalCandidates := 0

	if checker, ok := i.fs.(SnapshotStateChecker); ok {
		upToDate, err := checker.SnapshotUpToDate(ctx, norm)
		if err != nil {
			return nil, nil, err
		}
		if upToDate {
			fmt.Printf("embedius: index skip snapshot up-to-date location=%q\n", norm)
			return nil, nil, nil
		}
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
	ctx = WithIndexBase(ctx, baseNormalised)
	if logProgress {
		for _, object := range objects {
			objectPath := url.Path(object.URL())
			if url.Equals(objectPath, location) && object.IsDir() {
				continue
			}
			if i.matcher.IsExcluded(url.Path(object.URL()), int(object.Size())) {
				continue
			}
			if object.IsDir() {
				oUrl := object.URL()
				if baseNormalised == oUrl {
					continue
				}
				continue
			}
			totalCandidates++
		}
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
		if logProgress {
			processed++
			if processed%nextLog == 0 {
				fmt.Printf("embedius: index progress location=%q processed=%d total=%d\n", location, processed, totalCandidates)
			}
		}
	}
	if logProgress && totalCandidates > 0 && processed != totalCandidates {
		fmt.Printf("embedius: index progress location=%q processed=%d total=%d\n", location, processed, totalCandidates)
	}

	return toAddDocuments, toRemove, nil
}

func relativePath(ctx context.Context, object storage.Object) string {
	base := indexBase(ctx)
	if base == "" || object == nil {
		return ""
	}
	basePath := strings.TrimRight(url.Path(base), "/")
	objPath := url.Path(object.URL())
	if basePath == "" || objPath == "" {
		return ""
	}
	if !strings.HasPrefix(objPath, basePath) {
		return ""
	}
	rel := strings.TrimPrefix(objPath, basePath)
	rel = strings.TrimPrefix(rel, "/")
	return rel
}

// indexFile indexes a single file
func (i *Indexer) indexFile(ctx context.Context, object storage.Object, cache *cache.Map[string, document.Entry]) ([]schema.Document, []string, error) {
	docId := url.Path(object.URL())
	relPath := relativePath(ctx, object)
	if assets := existingAssets(ctx); assets != nil && relPath != "" {
		if meta, ok := assets[relPath]; ok {
			md5hex := ""
			if withMD5, ok := object.(interface{ MD5() string }); ok {
				md5hex = strings.TrimSpace(withMD5.MD5())
			}
			if meta.Size == object.Size() && (md5hex == "" || md5hex == meta.MD5) {
				return nil, nil, nil
			}
		}
	}
	if existing := existingMD5s(ctx); existing != nil {
		if withMD5, ok := object.(interface{ MD5() string }); ok {
			if md5hex := strings.TrimSpace(withMD5.MD5()); md5hex != "" && existing[md5hex] {
				return nil, nil, nil
			}
		}
	}
	data, err := i.fs.Download(ctx, object)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download %s: %w", docId, err)
	}

	dataHash, md5hex := computeHash(data)
	if dataHash == 0 {
		dataHash = uint64(object.ModTime().Unix())
	}
	if md5hex != "" {
		if existing := existingMD5s(ctx); existing != nil && existing[md5hex] {
			return nil, nil, nil
		}
	}

	prev, ok := cache.Get(docId)
	if ok {
		if prev.Hash == dataHash {
			return nil, nil, nil // No changes detected
		}
	}
	aSplitter := i.splitterFactory.GetSplitter(docId, len(data))
	// Create new entry

	assetID := docId
	if relPath != "" {
		assetID = relPath
	}
	entry := &document.Entry{
		ID:      docId,
		ModTime: object.ModTime(),
		Hash:    dataHash,
		Fragments: aSplitter.Split(data, map[string]interface{}{
			meta.DocumentID: docId,
			"path":          docId,
			"rel_path":      relPath,
			"asset_id":      assetID,
			"md5":           md5hex,
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
func computeHash(data []byte) (uint64, string) {
	if len(data) == 0 {
		return 0, ""
	}
	sum := md5.Sum(data)
	return binary.BigEndian.Uint64(sum[:8]), hex.EncodeToString(sum[:])
}
