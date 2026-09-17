package service

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/embedius/indexer/fs/splitter"
	"github.com/viant/embedius/metadata"
	"github.com/viant/sqlite-vec/engine"
)

func TestListFilesAssetIDUsesBaseNameAndMD5(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "docs")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	content := []byte("hello world")
	path := filepath.Join(sub, "README.md")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	items, err := listFiles(root, nil)
	if err != nil {
		t.Fatalf("listFiles: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 file, got %d", len(items))
	}
	item := items[0]
	expectedAssetID := "docs/README.md"
	if item.assetID != expectedAssetID {
		t.Fatalf("assetID mismatch: got %q want %q", item.assetID, expectedAssetID)
	}
	if strings.Contains(item.assetID, "\\") {
		t.Fatalf("assetID should not include backslashes: %q", item.assetID)
	}
}

type countingEmbedder struct{ calls int }

func (e *countingEmbedder) EmbedDocuments(_ context.Context, docs []string) ([][]float32, error) {
	e.calls += len(docs)
	result := make([][]float32, len(docs))
	for i := range result {
		result[i] = []float32{1, 0}
	}
	return result, nil
}

func (e *countingEmbedder) EmbedQuery(_ context.Context, _ string) ([]float32, error) {
	return []float32{1, 0}, nil
}

func TestIndexRefreshesConfiguredMetadataWithoutEmbedding(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "example.md"), []byte("---\ntitle: Example\nsourceUrl: https://example.test/example\n---\n\n# Example\n\nBody\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	embedder := &countingEmbedder{}
	svc, err := NewService(WithDSN(filepath.Join(t.TempDir(), "metadata_index.sqlite")), WithEmbedder(embedder))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()
	if err = svc.Index(ctx, IndexRequest{DBPath: filepath.Join(t.TempDir(), "unused.sqlite"), Roots: []RootSpec{{Name: "docs", Path: root}}, Embedder: embedder, Model: "test", ChunkSize: 64}); err != nil {
		t.Fatal(err)
	}
	beforeCalls := embedder.calls
	if beforeCalls == 0 {
		t.Fatal("expected initial embedding")
	}
	metadataConfig := metadata.Config{Extractor: "yaml-frontmatter", Fields: map[string]string{
		"document.title": "title",
		"source.url":     "sourceUrl",
	}}
	if err = svc.Index(ctx, IndexRequest{DBPath: filepath.Join(t.TempDir(), "unused.sqlite"), Roots: []RootSpec{{Name: "docs", Path: root, Metadata: metadataConfig}}, Embedder: embedder, Model: "test", ChunkSize: 64}); err != nil {
		t.Fatal(err)
	}
	if embedder.calls != beforeCalls {
		t.Fatalf("metadata-only refresh invoked embedder: before=%d after=%d", beforeCalls, embedder.calls)
	}
	var metaText string
	if err = svc.db.QueryRow(`SELECT meta FROM _vec_emb_docs WHERE dataset_id='docs' LIMIT 1`).Scan(&metaText); err != nil {
		t.Fatal(err)
	}
	var actual map[string]any
	if err = json.Unmarshal([]byte(metaText), &actual); err != nil {
		t.Fatal(err)
	}
	if actual["document.title"] != "Example" || actual["source.url"] != "https://example.test/example" {
		t.Fatalf("metadata refresh missing extracted values: %#v", actual)
	}
}

func TestUpsertRootConfig(t *testing.T) {
	db, err := engine.Open(t.TempDir() + "/root_config.sqlite")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := ensureSchema(context.Background(), db, "sqlite"); err != nil {
		t.Fatalf("ensureSchema: %v", err)
	}
	include := encodeGlobList([]string{"**/*.go", "**/*.sql"})
	exclude := encodeGlobList([]string{"**/*_test.go"})
	if err := upsertRootConfig(context.Background(), db, "mediator", include, exclude, 1024, "sqlite"); err != nil {
		t.Fatalf("upsertRootConfig: %v", err)
	}
	row := db.QueryRow(`SELECT include_globs, exclude_globs, max_size_bytes FROM emb_root_config WHERE dataset_id = ?`, "mediator")
	var gotInclude, gotExclude string
	var gotMax int64
	if err := row.Scan(&gotInclude, &gotExclude, &gotMax); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if gotInclude != include || gotExclude != exclude || gotMax != 1024 {
		t.Fatalf("unexpected values: include=%q exclude=%q max=%d", gotInclude, gotExclude, gotMax)
	}
}

func TestSplitFileCopiesDocumentMetadataToEveryFragment(t *testing.T) {
	docs, err := splitFile("docs/example.md", []byte("# Example\n\nBody\n"), splitter.NewFactory(32), map[string]any{
		"document.title": "Example",
		"source.url":     "https://example.test/docs/example",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) == 0 {
		t.Fatal("expected fragments")
	}
	for _, doc := range docs {
		if doc.Metadata["document.title"] != "Example" || doc.Metadata["source.url"] != "https://example.test/docs/example" {
			t.Fatalf("missing document metadata: %#v", doc.Metadata)
		}
	}
}

func TestRefreshAssetMetadataDoesNotReembed(t *testing.T) {
	db, err := engine.Open(t.TempDir() + "/metadata_refresh.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if err = ensureSchema(ctx, db, "sqlite"); err != nil {
		t.Fatal(err)
	}
	if err = ensureDataset(ctx, db, "docs", "/documents", "sqlite"); err != nil {
		t.Fatal(err)
	}
	beforeEmbedding := []byte{1, 2, 3, 4}
	beforeMeta := `{"path":"example.md","document_id":"example.md","fragment_id":"example.md:0-10"}`
	if _, err = db.Exec(`INSERT INTO _vec_emb_docs(dataset_id,id,asset_id,content,meta,embedding,embedding_model,scn,archived) VALUES(?,?,?,?,?,?,?,?,0)`, "docs", "example.md:0-10", "example.md", "Body", beforeMeta, beforeEmbedding, "test", 1); err != nil {
		t.Fatal(err)
	}
	changed, err := refreshAssetMetadata(ctx, db, "docs", "example.md", []string{"document.title", "source.url"}, map[string]any{
		"document.title": "Example",
		"source.url":     "https://example.test/example",
	}, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected metadata refresh")
	}
	var metaText string
	var embedding []byte
	if err = db.QueryRow(`SELECT meta,embedding FROM _vec_emb_docs WHERE dataset_id=? AND id=?`, "docs", "example.md:0-10").Scan(&metaText, &embedding); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(embedding, beforeEmbedding) {
		t.Fatalf("metadata refresh changed embedding: %v", embedding)
	}
	var actual map[string]any
	if err = json.Unmarshal([]byte(metaText), &actual); err != nil {
		t.Fatal(err)
	}
	if actual["document.title"] != "Example" || actual["source.url"] != "https://example.test/example" {
		t.Fatalf("metadata was not refreshed: %#v", actual)
	}
	changed, err = refreshAssetMetadata(ctx, db, "docs", "example.md", []string{"document.title", "source.url"}, map[string]any{
		"document.title": "Example",
		"source.url":     "https://example.test/example",
	}, "sqlite")
	if err != nil || changed {
		t.Fatalf("expected idempotent refresh, changed=%t err=%v", changed, err)
	}
}

func TestMetadataConfigPassesFrontmatterFields(t *testing.T) {
	actual, err := metadataForFile(RootSpec{Metadata: metadata.Config{
		Extractor: "yaml-frontmatter",
		Fields:    map[string]string{"document.title": "title"},
	}}, []byte("---\ntitle: Example\n---\n\n# Example\n"))
	if err != nil {
		t.Fatal(err)
	}
	if actual["document.title"] != "Example" {
		t.Fatalf("unexpected metadata: %#v", actual)
	}
}

func TestNewMatcher_ConfigGitignoreSemantics(t *testing.T) {
	spec := RootSpec{
		Exclude: []string{"/rootbuild", "tmp/"},
	}
	m := newMatcher(spec)
	if !m.IsExcluded("s3://bucket/rootbuild/app.js", 1) {
		t.Fatalf("expected /rootbuild to exclude root path")
	}
	if m.IsExcluded("s3://bucket/dir/rootbuild/app.js", 1) {
		t.Fatalf("expected /rootbuild to not exclude nested path")
	}
	if !m.IsExcluded("s3://bucket/dir/tmp/file.txt", 1) {
		t.Fatalf("expected tmp/ to exclude nested directory")
	}

	spec = RootSpec{
		Include: []string{"/docs/*.md"},
	}
	m = newMatcher(spec)
	if m.IsExcluded("s3://bucket/docs/readme.md", 1) {
		t.Fatalf("expected /docs/*.md to include root docs")
	}
	if !m.IsExcluded("s3://bucket/dir/docs/readme.md", 1) {
		t.Fatalf("expected /docs/*.md to not include nested docs")
	}
}
