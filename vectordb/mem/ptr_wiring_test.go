package mem

import (
	"context"
	"testing"

	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"
)

// Test that AddDocuments writes payloads to ValueStore and Set stores only Ptrs.
func TestAddDocumentsStoresPtrsAndWrites(t *testing.T) {
	set, err := NewSet(context.Background(), "", "ns")
	if err != nil {
		t.Fatalf("NewSet error: %v", err)
	}

	docs := []schema.Document{{PageContent: "Warsaw"}, {PageContent: "Paris"}, {PageContent: "Tokyo"}}
	embedder := &mockEmbedder{}
	opts := vectorstores.WithEmbedder(embedder)

	// Before stats
	pre := set.values.Stats()

	ids, err := set.AddDocuments(context.Background(), docs, opts)
	if err != nil {
		t.Fatalf("AddDocuments failed: %v", err)
	}
	if len(ids) != len(docs) {
		t.Fatalf("expected %d ids, got %d", len(docs), len(ids))
	}

	// Verify in-memory holds Ptrs only
	if len(set.ptrs) != len(docs) {
		t.Fatalf("expected %d ptrs, got %d", len(docs), len(set.ptrs))
	}
	if len(set.vectors) != len(docs) {
		t.Fatalf("expected %d vectors, got %d", len(docs), len(set.vectors))
	}
	// Ptrs should have non-zero Length
	var sumLen uint64
	for i, p := range set.ptrs {
		if p.Length == 0 {
			t.Errorf("ptr[%d].Length = 0, want > 0", i)
		}
		sumLen += uint64(p.Length)
	}

	// After stats
	post := set.values.Stats()
	if got, want := post.Appends-pre.Appends, uint64(len(docs)); got != want {
		t.Errorf("Appends delta = %d, want %d", got, want)
	}
	if got, want := post.BytesWritten-pre.BytesWritten, sumLen; got != want {
		t.Errorf("BytesWritten delta = %d, want %d (sum of ptr lengths)", got, want)
	}
}

// Test that SimilaritySearch materializes only top-K via ValueStore (reads == sum of K ptr lengths).
func TestSearchLazyMaterializationTopK(t *testing.T) {
	set, err := NewSet(context.Background(), "", "ns2")
	if err != nil {
		t.Fatalf("NewSet error: %v", err)
	}
	docs := []schema.Document{{PageContent: "Warsaw"}, {PageContent: "Paris"}, {PageContent: "Tokyo"}}
	embedder := &mockEmbedder{}
	opts := vectorstores.WithEmbedder(embedder)

	if _, err := set.AddDocuments(context.Background(), docs, opts); err != nil {
		t.Fatalf("AddDocuments failed: %v", err)
	}

	// Build a map from content to ptr length (insertion order matches docs)
	contentLen := map[string]uint64{
		"Warsaw": uint64(set.ptrs[0].Length),
		"Paris":  uint64(set.ptrs[1].Length),
		"Tokyo":  uint64(set.ptrs[2].Length),
	}

	// k = 1
	pre := set.values.Stats()
	res, err := set.SimilaritySearch(context.Background(), "Poland", 1, opts)
	if err != nil {
		t.Fatalf("SimilaritySearch k=1 failed: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 result, got %d", len(res))
	}
	post := set.values.Stats()
	delta := post.BytesRead - pre.BytesRead
	want := contentLen[res[0].PageContent]
	if delta != want {
		t.Errorf("BytesRead delta k=1 = %d, want %d for %q", delta, want, res[0].PageContent)
	}

	// k = 2
	pre2 := set.values.Stats()
	res2, err := set.SimilaritySearch(context.Background(), "Poland", 2, opts)
	if err != nil {
		t.Fatalf("SimilaritySearch k=2 failed: %v", err)
	}
	if len(res2) != 2 {
		t.Fatalf("expected 2 results, got %d", len(res2))
	}
	post2 := set.values.Stats()
	delta2 := post2.BytesRead - pre2.BytesRead
	want2 := contentLen[res2[0].PageContent] + contentLen[res2[1].PageContent]
	if delta2 != want2 {
		t.Errorf("BytesRead delta k=2 = %d, want %d for [%q,%q]", delta2, want2, res2[0].PageContent, res2[1].PageContent)
	}
}
