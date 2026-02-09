package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestUpsertRootConfig(t *testing.T) {
	db, err := engine.Open(t.TempDir() + "/root_config.sqlite")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := ensureSchema(context.Background(), db); err != nil {
		t.Fatalf("ensureSchema: %v", err)
	}
	include := encodeGlobList([]string{"**/*.go", "**/*.sql"})
	exclude := encodeGlobList([]string{"**/*_test.go"})
	if err := upsertRootConfig(context.Background(), db, "mediator", include, exclude, 1024); err != nil {
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
