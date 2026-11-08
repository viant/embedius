// Package bm25 provides corpus-fitted sparse embeddings and lexical search.
// Fit once when documents change; embeddings and indexes are immutable thereafter.
package bm25

import (
	"context"
	"github.com/viant/embedius/embeddings"
	"math"
	"slices"
)

// Vector contains term weights. Document and query vectors must come from the
// same fitted corpus. Their dot product is BM25, not cosine similarity.
type Vector = embeddings.SparseVector

var _ embeddings.SparseEmbedder = (*Embedder)(nil)

// Embedder holds immutable corpus statistics. It is safe for concurrent use.
type Embedder struct {
	averageLength float32
	idf           map[string]float32
}

// Fit computes document frequencies and length normalization for a corpus.
func Fit(ctx context.Context, documents []string) (*Embedder, error) {
	terms, err := analyzeDocuments(ctx, documents)
	if err != nil {
		return nil, err
	}
	return fitTerms(terms), nil
}

func analyzeDocuments(ctx context.Context, documents []string) ([][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([][]string, len(documents))
	for i, document := range documents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result[i] = defaultAnalyzer(document)
	}
	return result, nil
}

func fitTerms(documents [][]string) *Embedder {
	frequencies := map[string]int{}
	total := 0
	for _, terms := range documents {
		total += len(terms)
		seen := map[string]bool{}
		for _, term := range terms {
			if !seen[term] {
				frequencies[term]++
				seen[term] = true
			}
		}
	}

	e := &Embedder{averageLength: 256, idf: make(map[string]float32, len(frequencies))}
	if len(documents) > 0 && total > 0 {
		e.averageLength = float32(float64(total) / float64(len(documents)))
	}
	for term, count := range frequencies {
		df := float32(count)
		e.idf[term] = float32(math.Log(float64(1 + (float32(len(documents))-df+0.5)/(df+0.5))))
	}
	return e
}

// EmbedDocuments returns independent sparse document vectors with TF saturation,
// corpus IDF and length normalization. Unknown terms have zero contribution.
func (e *Embedder) EmbedDocuments(ctx context.Context, documents []string) ([]Vector, error) {
	terms, err := analyzeDocuments(ctx, documents)
	if err != nil {
		return nil, err
	}
	return e.embedTerms(terms), nil
}

func (e *Embedder) embedTerms(documents [][]string) []Vector {
	result := make([]Vector, len(documents))
	for i, terms := range documents {
		counts := map[string]int{}
		for _, term := range terms {
			counts[term]++
		}
		vector := Vector{}
		const k1, b = float32(1.2), float32(0.75)
		norm := float32(k1 * (1 - b + float32(b*(float32(len(terms))/e.averageLength))))
		for term, count := range counts {
			if idf, ok := e.idf[term]; ok {
				tf := float32(count)
				vector[term] = (float32(tf*(k1+1)) / (tf + norm)) * idf
			}
		}
		result[i] = vector
	}
	return result
}

// EmbedQuery counts query terms; repeated occurrences contribute repeatedly.
func (e *Embedder) EmbedQuery(ctx context.Context, query string) (Vector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := Vector{}
	for _, term := range defaultAnalyzer(query) {
		if _, ok := e.idf[term]; ok {
			result[term]++
		}
	}
	return result, nil
}

// Score computes the sparse dot product between a document and a query.
func Score(document, query Vector) float64 {
	var result float64
	terms := make([]string, 0, len(query))
	for term := range query {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	for _, term := range terms {
		result += float64(document[term]) * float64(query[term])
	}
	return result
}
