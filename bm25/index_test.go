package bm25

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestSparseScoringAndSearch(t *testing.T) {
	ctx := context.Background()
	texts := []string{"red fox fox", "blue bird", "red bird"}
	e, err := Fit(ctx, texts)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := e.EmbedDocuments(ctx, texts)
	if err != nil {
		t.Fatal(err)
	}
	query, _ := e.EmbedQuery(ctx, "fox fox")
	// Independent BM25 calculation: N=3, df=1, tf=2, dl=3, avgdl=7/3.
	want := 2 * math.Log(1+(3-1+0.5)/(1+0.5)) * 2 * 2.2 / (2 + 1.2*(.25+.75*3/(7.0/3)))
	if got := Score(docs[0], query); math.Abs(got-want) > 1e-6 {
		t.Fatalf("got %v want %v", got, want)
	}
	idx, err := New(ctx, []Document{{"a", texts[0]}, {"b", texts[1]}, {"c", texts[2]}})
	if err != nil {
		t.Fatal(err)
	}
	hits, err := idx.Search(ctx, "fox fox", 7)
	if err != nil || len(hits) != 1 || hits[0].ID != "a" || math.Abs(hits[0].Score-want) > 1e-6 {
		t.Fatalf("%v %v", hits, err)
	}
	docs[0]["fox"] = 999
	hits[0].ID = "changed"
	again, _ := idx.Search(ctx, "fox", 7)
	if again[0].ID != "a" || again[0].Score > 5 {
		t.Fatal("mutable result corrupted index")
	}
}
func TestNormalizationEmptyAndTies(t *testing.T) {
	ctx := context.Background()
	idx, err := New(ctx, []Document{{"first", "Café running"}, {"second", "Cafe running"}, {"empty", "the and"}})
	if err != nil {
		t.Fatal(err)
	}
	hits, _ := idx.Search(ctx, "cafe run", 0)
	if len(hits) != 2 || hits[0].ID != "first" || hits[1].ID != "second" {
		t.Fatal(hits)
	}
	for _, q := range []string{"the and", "unknownword", ""} {
		h, e := idx.Search(ctx, q, 7)
		if e != nil || len(h) != 0 {
			t.Fatalf("%q: %v %v", q, h, e)
		}
	}
	empty, _ := New(ctx, nil)
	h, e := empty.Search(ctx, "fox", 7)
	if e != nil || len(h) != 0 {
		t.Fatal(h, e)
	}
	for _, ds := range [][]Document{{{"", "text"}}, {{"a", "one"}, {"a", "two"}}} {
		if _, e := New(ctx, ds); e == nil {
			t.Fatal("invalid identifier accepted")
		}
	}
}
func TestConcurrentReadersAndCancellation(t *testing.T) {
	ctx := context.Background()
	idx, _ := New(ctx, []Document{{"one", "cache catalog"}, {"two", "load file"}})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h, e := idx.Search(ctx, "catalog", 7)
				if e != nil || len(h) != 1 || h[0].ID != "one" {
					t.Error(h, e)
					return
				}
			}
		}()
	}
	wg.Wait()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := idx.Search(cancelled, "catalog", 7); e == nil {
		t.Fatal("cancellation ignored")
	}
	if _, e := Fit(cancelled, []string{"text"}); e == nil {
		t.Fatal("fit cancellation ignored")
	}
	replacement, _ := New(ctx, []Document{{"two", "file search"}})
	if h, _ := replacement.Search(ctx, "catalog", 7); len(h) != 0 {
		t.Fatal("removed document retained")
	}
}
func BenchmarkSearch(b *testing.B) {
	docs := make([]Document, 500)
	for i := range docs {
		docs[i] = Document{fmt.Sprint(i), fmt.Sprintf("Search document category%d retrieve cached metadata", i%20)}
	}
	idx, _ := New(context.Background(), docs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.Search(context.Background(), "cached metadata category7", 7)
	}
}
