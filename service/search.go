package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/viant/embedius/db/sqliteutil"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vec"
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

	return s.searchWithConn(ctx, conn, req, emb, db)
}

func (s *Service) SearchWithConn(ctx context.Context, conn *sql.Conn, req SearchRequest) ([]SearchResult, error) {
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
	return s.searchWithConn(ctx, conn, req, emb, nil)
}

func (s *Service) searchWithConn(ctx context.Context, conn *sql.Conn, req SearchRequest, emb embeddings.Embedder, db *sql.DB) ([]SearchResult, error) {
	vecs, err := emb.EmbedDocuments(ctx, []string{req.Query})
	if err != nil {
		return nil, err
	}
	if len(vecs) != 1 {
		return nil, fmt.Errorf("embedder returned %d vectors for 1 query", len(vecs))
	}
	qvec := vecs[0]
	return s.searchWithPrepared(ctx, conn, req, qvec, db)
}

// SearchWithEmbedding performs a search using a precomputed query embedding.
func (s *Service) SearchWithEmbedding(ctx context.Context, req SearchRequest, qvec []float32) ([]SearchResult, error) {
	if req.Dataset == "" || req.Query == "" {
		return nil, fmt.Errorf("dataset and query are required")
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	if len(qvec) == 0 {
		return nil, fmt.Errorf("query embedding is required")
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
	return s.searchWithPrepared(ctx, conn, req, qvec, db)
}

// SearchWithConnAndEmbedding performs a search using a precomputed embedding and existing connection.
func (s *Service) SearchWithConnAndEmbedding(ctx context.Context, conn *sql.Conn, req SearchRequest, qvec []float32) ([]SearchResult, error) {
	if req.Dataset == "" || req.Query == "" {
		return nil, fmt.Errorf("dataset and query are required")
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	if len(qvec) == 0 {
		return nil, fmt.Errorf("query embedding is required")
	}
	return s.searchWithPrepared(ctx, conn, req, qvec, nil)
}

func (s *Service) searchWithPrepared(ctx context.Context, conn *sql.Conn, req SearchRequest, qvec []float32, db *sql.DB) ([]SearchResult, error) {
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
	if err != nil {
		if db != nil && strings.Contains(err.Error(), "no such module: vec") {
			if rows, err := s.matchWithFreshDB(ctx, req, blob); err == nil {
				return rows, nil
			}
		}
		if err != nil && (strings.Contains(err.Error(), "no such module: vec") ||
			strings.Contains(err.Error(), "no such table: emb_docs") ||
			strings.Contains(err.Error(), "unable to use function MATCH")) {
			fmt.Printf("embedius: MATCH unavailable, falling back to brute-force search: %v\n", err)
			return fallbackSearch(ctx, conn, req.Dataset, qvec, req.MinScore, req.Limit)
		}
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	return scanSearchRows(rows)
}

func (s *Service) matchWithFreshDB(ctx context.Context, req SearchRequest, blob []byte) ([]SearchResult, error) {
	db, err := engine.Open(sqliteutil.EnsurePragmas(req.DBPath, true, 5000))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	if err := vec.Register(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	conn, err := ensureSchemaConn(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	defer func() {
		_ = conn.Close()
		_ = db.Close()
	}()
	rows, err := conn.QueryContext(ctx, `SELECT d.id, v.match_score, d.content, d.meta
FROM emb_docs v
JOIN _vec_emb_docs d ON d.dataset_id = v.dataset_id AND d.id = v.doc_id
WHERE v.dataset_id = ?
  AND v.doc_id MATCH ?
  AND d.archived = 0
  AND v.match_score >= ?
ORDER BY v.match_score DESC
LIMIT ?`, req.Dataset, blob, req.MinScore, req.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSearchRows(rows)
}

func scanSearchRows(rows *sql.Rows) ([]SearchResult, error) {
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
