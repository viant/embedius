package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/sqlite-vec/vector"
)

// Search performs a vector search for the given dataset.
func (s *Service) Search(ctx context.Context, req SearchRequest) ([]SearchResult, error) {
	if req.Dataset == "" || req.Query == "" {
		return nil, fmt.Errorf("dataset and query are required")
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	emb, err := s.resolveEmbedder(req.Embedder)
	if err != nil {
		return nil, err
	}

	db, err := s.ensureDB(ctx, req.DBPath, false)
	if err != nil {
		return nil, err
	}
	conn, err := ensureSchemaConn(ctx, db)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	vecs, err := emb.EmbedDocuments(ctx, []string{req.Query})
	if err != nil {
		return nil, err
	}
	if len(vecs) != 1 {
		return nil, fmt.Errorf("embedder returned %d vectors for 1 query", len(vecs))
	}
	qvec := vecs[0]
	blob, err := vector.EncodeEmbedding(qvec)
	if err != nil {
		return nil, err
	}

	rows, err := conn.QueryContext(ctx, `SELECT d.id, v.match_score, d.content, d.meta
FROM emb_docs v
JOIN _vec_emb_docs d ON d.dataset_id = v.dataset_id AND d.id = v.doc_id
WHERE v.dataset_id = ?
  AND v.doc_id MATCH ?
  AND d.archived = 0
  AND v.match_score >= ?
ORDER BY v.match_score DESC
LIMIT ?`, req.Dataset, blob, req.MinScore, req.Limit)
	if err != nil && (strings.Contains(err.Error(), "no such module: vec") ||
		strings.Contains(err.Error(), "no such table: emb_docs") ||
		strings.Contains(err.Error(), "unable to use function MATCH")) {
		return fallbackSearch(ctx, conn, req.Dataset, qvec, req.MinScore, req.Limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SearchResult
	for rows.Next() {
		var item SearchResult
		if err := rows.Scan(&item.ID, &item.Score, &item.Content, &item.Meta); err != nil {
			return nil, err
		}
		item.Path = extractMetaPath(item.Meta)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
