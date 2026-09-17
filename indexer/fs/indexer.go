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
	"sync"

	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/embedius/indexer/fs/splitter"
	"github.com/viant/embedius/matching"
	"github.com/viant/embedius/metadata"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectordb/meta"
)

// Indexer implements indexing for filesystem resources
type Indexer struct {
	fs               Service
	baseURL          string
	matcher          *matching.Manager
	splitterFactory  *splitter.Factory
	embeddingsModel  string
	metadataResolver MetadataResolver
	metadataMu       sync.Mutex
	metadataChanged  bool
}

// MetadataResolver returns document metadata extraction configuration for a
// root or file URI. The resolver is optional; a nil resolver preserves the
// existing path/document-id-only metadata behavior.
type MetadataResolver func(context.Context, string) metadata.Config

// Option configures a filesystem indexer.
type Option func(*Indexer)

// WithMetadataResolver attaches an optional metadata extraction resolver.
func WithMetadataResolver(resolver MetadataResolver) Option {
	return func(i *Indexer) { i.metadataResolver = resolver }
}

// New creates a new filesystem indexer
func New(baseURL string, embeddingsModel string, matcher *matching.Manager, splitterFactory *splitter.Factory, options ...Option) *Indexer {
	result := &Indexer{
		fs:              NewAFS(),
		baseURL:         baseURL,
		matcher:         matcher,
		embeddingsModel: embeddingsModel,
		splitterFactory: splitterFactory,
	}
	for _, option := range options {
		if option != nil {
			option(result)
		}
	}
	return result
}

// NewWithFS creates a new filesystem indexer with a custom FS service implementation.
func NewWithFS(baseURL string, embeddingsModel string, matcher *matching.Manager, splitterFactory *splitter.Factory, fsSvc Service, options ...Option) *Indexer {
	if fsSvc == nil {
		fsSvc = NewAFS()
	}
	result := &Indexer{
		fs:              fsSvc,
		baseURL:         baseURL,
		matcher:         matcher,
		embeddingsModel: embeddingsModel,
		splitterFactory: splitterFactory,
	}
	for _, option := range options {
		if option != nil {
			option(result)
		}
	}
	return result
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
	var stats *IndexStats
	if logProgress {
		stats = indexStats(ctx)
		if stats == nil {
			stats = &IndexStats{}
			ctx = WithIndexStats(ctx, stats)
		}
	}

	if checker, ok := i.fs.(SnapshotStateChecker); ok {
		upToDate, err := checker.SnapshotUpToDate(ctx, norm)
		if err != nil {
			return nil, nil, err
		}
		if upToDate {
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
		if stats != nil {
			stats.Total = totalCandidates
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
			if stats != nil {
				stats.Processed = processed
				if len(docs) == 0 && len(ids) == 0 {
					stats.Unchanged++
				} else {
					stats.Changed++
					stats.Docs += len(docs)
				}
			}
			if processed%nextLog == 0 {
				_ = stats
			}
		}
	}
	if logProgress && totalCandidates > 0 && processed != totalCandidates {
		_ = stats
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
	metadataConfig := i.metadataConfig(ctx, docId)
	metadataEnabled := metadataConfig.Enabled()
	relPath := relativePath(ctx, object)
	if !metadataEnabled {
		if assets := existingAssets(ctx); assets != nil && relPath != "" {
			if meta, ok := assets[relPath]; ok {
				md5hex := ""
				if withMD5, ok := object.(interface{ MD5() string }); ok {
					md5hex = strings.TrimSpace(withMD5.MD5())
				}
				// If upstream md5 matches, treat as unchanged even when size is unknown or mismatched.
				if md5hex != "" && meta.MD5 != "" && strings.EqualFold(md5hex, meta.MD5) {
					return nil, nil, nil
				}
				if meta.Size == object.Size() && (md5hex == "" || md5hex == meta.MD5) {
					return nil, nil, nil
				}
			}
		}
	}
	if !metadataEnabled {
		if existing := existingMD5s(ctx); existing != nil {
			if withMD5, ok := object.(interface{ MD5() string }); ok {
				if md5hex := strings.TrimSpace(withMD5.MD5()); md5hex != "" && existing[md5hex] {
					return nil, nil, nil
				}
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
	if !metadataEnabled && md5hex != "" {
		if existing := existingMD5s(ctx); existing != nil && existing[md5hex] {
			return nil, nil, nil
		}
	}

	documentMetadata, err := metadataConfig.Extract(data)
	if err != nil {
		return nil, nil, fmt.Errorf("extract metadata from %s: %w", docId, err)
	}
	fragmentMetadata := metadata.StringValues(documentMetadata)
	prev, ok := cache.Get(docId)
	if ok {
		if prev.Hash == dataHash {
			if mergeFragmentMetadata(prev.Fragments, fragmentMetadata) {
				cache.Set(docId, prev)
				i.markMetadataChanged()
			}
			return nil, nil, nil // No changes detected
		}
	}
	aSplitter := i.splitterFactory.GetSplitter(docId, len(data))
	// Create new entry

	assetID := docId
	if relPath != "" {
		assetID = relPath
	}
	content := data
	var fragments []*document.Fragment
	metaMap := map[string]interface{}{
		meta.DocumentID: docId,
		"path":          docId,
		"rel_path":      relPath,
		"asset_id":      assetID,
		"md5":           md5hex,
	}
	if cs, ok := aSplitter.(splitter.ContentSplitter); ok {
		fragments, content = cs.SplitWithContent(data, metaMap)
	} else {
		fragments = aSplitter.Split(data, metaMap)
	}
	if content == nil {
		content = data
	}
	mergeFragmentMetadata(fragments, fragmentMetadata)
	entry := &document.Entry{
		ID:        docId,
		ModTime:   object.ModTime(),
		Hash:      dataHash,
		Fragments: fragments,
	}
	cache.Set(docId, entry)

	// Create documents from fragments
	var documents []schema.Document
	for _, fragment := range entry.Fragments {
		documents = append(documents, fragment.NewDocument(docId, content))
	}

	// Determine IDs to remove (from previous version)
	var toRemove []string
	if prev != nil {
		toRemove = prev.Fragments.VectorDBIDs()
	}
	return documents, toRemove, nil
}

// ConsumeMetadataChanges reports and clears metadata-only cache changes.
func (i *Indexer) ConsumeMetadataChanges() bool {
	if i == nil {
		return false
	}
	i.metadataMu.Lock()
	defer i.metadataMu.Unlock()
	changed := i.metadataChanged
	i.metadataChanged = false
	return changed
}

func (i *Indexer) markMetadataChanged() {
	i.metadataMu.Lock()
	defer i.metadataMu.Unlock()
	i.metadataChanged = true
}

func (i *Indexer) metadataConfig(ctx context.Context, uri string) metadata.Config {
	if i == nil || i.metadataResolver == nil {
		return metadata.Config{}
	}
	return i.metadataResolver(ctx, uri)
}

func mergeFragmentMetadata(fragments document.Fragments, values map[string]string) bool {
	if len(values) == 0 {
		return false
	}
	changed := false
	for _, fragment := range fragments {
		if fragment == nil {
			continue
		}
		if fragment.Meta == nil {
			fragment.Meta = map[string]string{}
		}
		for key, value := range values {
			if fragment.Meta[key] == value {
				continue
			}
			fragment.Meta[key] = value
			changed = true
		}
	}
	return changed
}

// computeHash computes a hash for the given data
func computeHash(data []byte) (uint64, string) {
	if len(data) == 0 {
		return 0, ""
	}
	sum := md5.Sum(data)
	return binary.BigEndian.Uint64(sum[:8]), hex.EncodeToString(sum[:])
}
