package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/embedius/indexer/fs/splitter"
	"github.com/viant/embedius/matching"
	"github.com/viant/embedius/metadata"
)

func TestMetadataRefreshDoesNotReindexUnchangedFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "example.md")
	if err := os.WriteFile(path, []byte("---\ntitle: Example\nsourceUrl: https://example.test/example\n---\n\n# Example\n\nBody\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := cache.NewMap[string, document.Entry]()
	// First index mirrors the already-indexed corpus: no configured extractor.
	plain := New(root, "test", matching.New(), splitter.NewFactory(64))
	docs, removed, err := plain.Index(context.Background(), root, cache)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) == 0 || len(removed) != 0 {
		t.Fatalf("unexpected initial index result docs=%d removed=%d", len(docs), len(removed))
	}

	configured := New(root, "test", matching.New(), splitter.NewFactory(64), WithMetadataResolver(func(context.Context, string) metadata.Config {
		return metadata.Config{Extractor: "yaml-frontmatter", Fields: map[string]string{
			"document.title": "title",
			"source.url":     "sourceUrl",
		}}
	}))
	docs, removed, err = configured.Index(context.Background(), root, cache)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 0 || len(removed) != 0 {
		t.Fatalf("metadata refresh must not create vector work: docs=%d removed=%d", len(docs), len(removed))
	}
	if !configured.ConsumeMetadataChanges() {
		t.Fatal("expected metadata-only cache change")
	}
	entry, ok := cache.Get(path)
	if !ok || len(entry.Fragments) == 0 {
		t.Fatal("expected cached fragments")
	}
	if entry.Fragments[0].Meta["document.title"] != "Example" || entry.Fragments[0].Meta["source.url"] != "https://example.test/example" {
		t.Fatalf("metadata not attached to cached fragment: %#v", entry.Fragments[0].Meta)
	}
}
