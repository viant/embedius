# In-memory BM25

This package provides local lexical ranking without a model service or database.
It uses Okapi BM25 (`k1=1.2`, `b=0.75`), positive Lucene-style IDF, English
stop-word removal and stemming, Unicode word segmentation, and transliteration.
Repeated query terms contribute repeatedly. Equal scores preserve corpus order.

```go
index, err := bm25.New(ctx, []bm25.Document{
    {ID: "files", Text: "Search local files by name and content"},
    {ID: "catalog", Text: "Find catalog entries by description"},
})
if err != nil { return err }
hits, err := index.Search(ctx, "search files", 7)
```

Build once from the current corpus. Reuse the immutable index for concurrent
queries. Build a replacement when documents change, then publish that replacement
using the caller's existing snapshot lifecycle. Results are independent copies.
The package does not own authorization, discovery, global caches, or invalidation.
Use one fitted corpus for results whose scores will be compared.

`Fit` returns an immutable embedder implementing `embeddings.SparseEmbedder`.
`EmbedDocuments` returns document weights and `EmbedQuery` returns query term
counts. `Score` computes their sparse dot product. These are lexical embeddings,
not dense vectors for cosine similarity. Refit after changes to the corpus;
unknown terms contribute nothing. Sparse dot products and indexed accumulation
may differ slightly due to floating-point accumulation order.

`Search` with a non-positive limit returns all matching documents. Empty,
stop-word-only, and unmatched queries return no results. Cancellation is supported.
No existing dense-vector or document-search default is changed by this package.

Run `go test -race ./bm25` and `go test ./bm25 -bench . -benchmem`.
See `NOTICE` and the included licenses for language-data attribution.
