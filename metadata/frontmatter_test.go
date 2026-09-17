package metadata

import (
	"testing"
)

func TestConfigExtractYAMLFrontmatter(t *testing.T) {
	config := Config{
		Extractor: "yaml-frontmatter",
		Fields: map[string]string{
			"document.title":   "title",
			"source.url":       "sourceUrl",
			"source.updatedAt": "sourceUpdatedAt",
		},
	}
	actual, err := config.Extract([]byte("---\ntitle: Example\nsourceUrl: https://example.test/doc\nsourceUpdatedAt: 2026-09-17T00:00:00Z\nignored: value\n---\n\n# Example\n"))
	if err != nil {
		t.Fatal(err)
	}
	if actual["document.title"] != "Example" || actual["source.url"] != "https://example.test/doc" {
		t.Fatalf("unexpected metadata: %#v", actual)
	}
	if _, ok := actual["ignored"]; ok {
		t.Fatalf("unexpected unmapped metadata: %#v", actual)
	}
}

func TestConfigExtractYAMLFrontmatterWithoutHeader(t *testing.T) {
	actual, err := (Config{Extractor: "yaml-frontmatter", Fields: map[string]string{"document.title": "title"}}).Extract([]byte("# Plain document\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(actual) != 0 {
		t.Fatalf("expected no metadata, got %#v", actual)
	}
}
