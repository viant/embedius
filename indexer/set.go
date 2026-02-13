package indexer

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/document"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/embedius/indexer/fs"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectordb"
	"github.com/viant/embedius/vectordb/meta"
	"github.com/viant/embedius/vectorstores"
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

type md5Lookup interface {
	ExistingAssetMD5s(ctx context.Context, dataset string, md5s []string) (map[string]bool, error)
}

type md5Lister interface {
	AssetMD5s(ctx context.Context, dataset string) (map[string]bool, error)
}

type assetMetaLister interface {
	AssetMetaByPath(ctx context.Context, dataset string) (map[string]fs.AssetMeta, error)
}

// SimilaritySearch performs similarity search against the vector vectorDb
func (s *Set) SimilaritySearch(ctx context.Context, query string, numDocuments int, opts ...vectorstores.Option) ([]schema.Document, error) {
	opts = s.ensureEmbedder(opts)
	documents, err := s.vectorDb.SimilaritySearch(ctx, query, numDocuments, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to search vector vectorDb: %w", err)
	}
	return documents, nil
}

// ensureEmbedder ensures the vector vectorDb options include an embedder
func (s *Set) ensureEmbedder(opts []vectorstores.Option) []vectorstores.Option {
	return append(s.vectorOptions, opts...)
}

// Index indexes content at the specified URI
func (s *Set) Index(ctx context.Context, URI string) error {
	start := time.Now()
	syncRequested := upstreamSyncConfig(ctx) != nil
	fmt.Printf("embedius: index start location=%q namespace=%q sync=%t\n", URI, s.namespace, syncRequested)
	ctx = fs.WithIndexRoot(ctx, URI)
	if cfg := upstreamSyncConfig(ctx); cfg != nil {
		if syncer, ok := s.vectorDb.(vectordb.UpstreamSyncer); ok {
			cfgCopy := *cfg
			if strings.TrimSpace(cfgCopy.DatasetID) == "" {
				cfgCopy.DatasetID = s.datasetName()
			}
			if err := syncer.SyncUpstream(ctx, cfgCopy); err != nil {
				return fmt.Errorf("upstream sync failed: %w", err)
			}
		}
	}
	if lister, ok := s.vectorDb.(md5Lister); ok {
		md5s, err := lister.AssetMD5s(ctx, s.datasetName())
		if err != nil {
			return fmt.Errorf("load md5 set failed: %w", err)
		}
		if len(md5s) > 0 {
			ctx = fs.WithExistingMD5s(ctx, md5s)
		}
	}
	if lister, ok := s.vectorDb.(assetMetaLister); ok {
		assets, err := lister.AssetMetaByPath(ctx, s.datasetName())
		if err != nil {
			return fmt.Errorf("load asset meta failed: %w", err)
		}
		if len(assets) > 0 {
			ctx = fs.WithExistingAssets(ctx, assets)
		}
	}
	toAddDocuments, toRemove, err := s.indexer.Index(ctx, URI, s.cache)
	if err != nil {
		fmt.Printf("embedius: index error location=%q err=%v\n", URI, err)
		return fmt.Errorf("indexing failed: %w", err)
	}

	if len(toAddDocuments) == 0 && len(toRemove) == 0 {
		fmt.Printf("embedius: index no-op location=%q duration=%s sync=%t\n", URI, time.Since(start), syncRequested)
		return nil // Nothing changed
	}

	// Skip embedding documents whose md5 already exists in the dataset.
	if lookup, ok := s.vectorDb.(md5Lookup); ok && len(toAddDocuments) > 0 {
		dataset := s.datasetName()
		md5ByDoc := map[string]string{}
		md5s := make([]string, 0, len(toAddDocuments))
		for _, doc := range toAddDocuments {
			if doc.Metadata == nil {
				continue
			}
			md5hex, _ := doc.Metadata["md5"].(string)
			docID, _ := doc.Metadata[meta.DocumentID].(string)
			md5hex = strings.TrimSpace(md5hex)
			if md5hex == "" || docID == "" {
				continue
			}
			if _, ok := md5ByDoc[docID]; ok {
				continue
			}
			md5ByDoc[docID] = md5hex
			md5s = append(md5s, md5hex)
		}
		if len(md5s) > 0 {
			existing, err := lookup.ExistingAssetMD5s(ctx, dataset, md5s)
			if err != nil {
				return fmt.Errorf("md5 lookup failed: %w", err)
			}
			if len(existing) > 0 {
				skipDocs := map[string]bool{}
				for docID, md5hex := range md5ByDoc {
					if existing[md5hex] {
						skipDocs[docID] = true
					}
				}
				if len(skipDocs) > 0 {
					filtered := make([]schema.Document, 0, len(toAddDocuments))
					skipped := 0
					for _, doc := range toAddDocuments {
						docID, _ := doc.Metadata[meta.DocumentID].(string)
						if skipDocs[docID] {
							skipped++
							continue
						}
						filtered = append(filtered, doc)
					}
					toAddDocuments = filtered
					if skipped > 0 {
						fmt.Printf("embedius: index skip location=%q md5_existing=%d\n", URI, skipped)
					}
					if len(toRemove) > 0 {
						skipIDs := map[string]bool{}
						for docID := range skipDocs {
							entry, ok := s.cache.Get(docID)
							if !ok || entry == nil {
								continue
							}
							for _, id := range entry.Fragments.VectorDBIDs() {
								if id != "" {
									skipIDs[id] = true
								}
							}
						}
						if len(skipIDs) > 0 {
							filteredRemove := make([]string, 0, len(toRemove))
							for _, id := range toRemove {
								if skipIDs[id] {
									continue
								}
								filteredRemove = append(filteredRemove, id)
							}
							toRemove = filteredRemove
						}
					}
				}
			}
		}
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

	fmt.Printf("embedius: index done location=%q added=%d removed=%d\n", URI, len(toAddDocuments), len(toRemove))
	return s.Persist(ctx)
}

func (s *Set) datasetName() string {
	opts := vectorstores.Options{}
	for _, opt := range s.vectorOptions {
		opt(&opts)
	}
	if strings.TrimSpace(opts.NameSpace) != "" {
		return opts.NameSpace
	}
	return "default"
}

// updateEntriesWithIDs updates entries with IDs returned from the vector vectorDb
func (s *Set) updateEntriesWithIDs(documents []schema.Document, ids []string) error {
	for i, id := range ids {
		if i >= len(documents) || id == "" {
			continue
		}
		doc := documents[i]
		metadata := doc.Metadata
		docID, ok := metadata[meta.DocumentID].(string)
		if !ok {
			fmt.Printf(meta.DocumentID+" not found in metadata for document %+v\n", metadata)
			continue
		}

		entry, ok := s.cache.Get(docID)
		if !ok {
			return fmt.Errorf("entry not found for document ID %s", docID)
		}

		fragment := entry.Fragments[0]
		if len(entry.Fragments) > 1 { //more than one fragment use fragmentID
			fragmentID, ok := metadata[meta.FragmentID].(string)
			if !ok {
				fmt.Printf(meta.FragmentID+" not found in metadata for document %+v\n", metadata)
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
		strings.Replace(strings.Replace(upstream.URL(), ".json", ".tr2", 1), "cache_", "index_", 1),
	}
	var destURL = []string{
		downstream.URL(),
		strings.Replace(strings.Replace(downstream.URL(), ".json", ".tr2", 1), "cache_", "index_", 1),
	}

	var dataByURL = map[string][]byte{}
	for _, URL := range sourceURL {
		data, err := s.fs.DownloadWithURL(ctx, URL)
		if err != nil {
			return nil //we do not report error here, as it is not critical - we just do not have syncronization from upstream
		}
		dataByURL[URL] = data
	}
	type mover interface {
		Move(context.Context, string, string) error
	}
	mv, hasMove := any(s.fs).(mover)
	for i, URL := range sourceURL {
		final := destURL[i]
		tmp := final + ".tmp"
		// Upload to temp first
		if err := s.fs.Upload(ctx, tmp, file.DefaultFileOsMode, bytes.NewReader(dataByURL[URL])); err != nil {
			return fmt.Errorf("failed to upload temp asset: %w", err)
		}
		// Try server-side move if supported
		if hasMove {
			if err := mv.Move(ctx, tmp, final); err != nil {
				// Fallback: re-upload directly to final, then delete temp
				if err2 := s.fs.Upload(ctx, final, file.DefaultFileOsMode, bytes.NewReader(dataByURL[URL])); err2 != nil {
					_ = s.fs.Delete(ctx, tmp)
					return fmt.Errorf("failed to move asset and upload fallback: %v / %v", err, err2)
				}
				_ = s.fs.Delete(ctx, tmp)
			}
		} else {
			// Fallback: re-upload directly to final, then delete temp
			if err := s.fs.Upload(ctx, final, file.DefaultFileOsMode, bytes.NewReader(dataByURL[URL])); err != nil {
				_ = s.fs.Delete(ctx, tmp)
				return fmt.Errorf("failed atomic fallback upload: %w", err)
			}
			_ = s.fs.Delete(ctx, tmp)
		}
	}
	// Remove legacy files at destination to avoid confusion alongside .tr2
	legacyTre := strings.Replace(strings.Replace(downstream.URL(), ".json", ".tre", 1), "cache_", "index_", 1)
	legacyDat := strings.Replace(strings.Replace(downstream.URL(), ".json", ".dat", 1), "cache_", "index_", 1)
	if exists, _ := s.fs.Exists(ctx, legacyTre); exists {
		_ = s.fs.Delete(ctx, legacyTre)
	}
	if exists, _ := s.fs.Exists(ctx, legacyDat); exists {
		_ = s.fs.Delete(ctx, legacyDat)
	}
	// Sync mmap-backed data files under data/<namespace>
	upstreamDataDir := url.Join(s.upstreamURL, fmt.Sprintf("data/%s", s.namespace))
	downstreamDataDir := url.Join(s.baseURL, fmt.Sprintf("data/%s", s.namespace))
	objects, _ := s.fs.List(ctx, upstreamDataDir)
	for _, object := range objects {
		// Only copy regular files (manifest.json, data_*.vdat)
		if object.IsDir() {
			continue
		}
		relName := strings.TrimPrefix(object.URL(), upstreamDataDir+"/")
		if relName == "writer.lock" {
			continue // never copy lock files
		}
		target := url.Join(downstreamDataDir, relName)
		data, err := s.fs.DownloadWithURL(ctx, object.URL())
		if err != nil {
			continue
		}
		if err := s.fs.Upload(ctx, target, file.DefaultFileOsMode, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("failed to sync data file %s: %w", relName, err)
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
