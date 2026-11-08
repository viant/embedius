package bm25

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Document is a stable identifier and searchable text.
type Document struct {
	ID   string
	Text string
}

// Match contains an identifier and a positive relevance score, higher first.
type Match struct {
	ID    string
	Score float64
}
type posting struct {
	id     int
	weight float32
}
type scratch struct {
	scores  []float32
	touched []int
}

// Index owns a fitted immutable corpus. Replace the index after publication or
// document changes; no document decoding or corpus fitting occurs in Search.
type Index struct {
	ids      []string
	postings map[string][]posting
	scratch  sync.Pool
}

// New constructs an index, rejecting duplicate or empty document identifiers.
func New(ctx context.Context, documents []Document) (*Index, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	texts := make([]string, len(documents))
	ids := make([]string, len(documents))
	seen := map[string]bool{}
	for i, d := range documents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if d.ID == "" || seen[d.ID] {
			return nil, fmt.Errorf("bm25: duplicate or empty document identifier")
		}
		seen[d.ID] = true
		ids[i] = d.ID
		texts[i] = d.Text
	}
	terms, err := analyzeDocuments(ctx, texts)
	if err != nil {
		return nil, err
	}
	e, err := fitTerms(ctx, terms)
	if err != nil {
		return nil, err
	}
	vectors, err := e.embedTerms(ctx, terms)
	if err != nil {
		return nil, err
	}
	idx := &Index{ids: ids, postings: map[string][]posting{}}
	for id, v := range vectors {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for term, w := range v {
			idx.postings[term] = append(idx.postings[term], posting{id, w})
		}
	}
	idx.scratch.New = func() any { return &scratch{scores: make([]float32, len(ids))} }
	return idx, ctx.Err()
}

// Search returns at most limit results. A non-positive limit returns all matches.
// Ties preserve corpus order. Returned results never alias index storage.
func (idx *Index) Search(ctx context.Context, query string, limit int) ([]Match, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	work := idx.scratch.Get().(*scratch)
	defer func() {
		for _, id := range work.touched {
			work.scores[id] = 0
		}
		work.touched = work.touched[:0]
		idx.scratch.Put(work)
	}()
	for _, term := range defaultAnalyzer(query) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, p := range idx.postings[term] {
			if work.scores[p.id] == 0 {
				work.touched = append(work.touched, p.id)
			}
			work.scores[p.id] += p.weight
		}
	}
	slices.SortFunc(work.touched, func(a, b int) int {
		if work.scores[a] > work.scores[b] {
			return -1
		}
		if work.scores[a] < work.scores[b] {
			return 1
		}
		return a - b
	})
	n := len(work.touched)
	if limit > 0 && limit < n {
		n = limit
	}
	result := make([]Match, n)
	for i, id := range work.touched[:n] {
		result[i] = Match{idx.ids[id], float64(work.scores[id])}
	}
	return result, ctx.Err()
}
