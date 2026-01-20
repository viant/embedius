package service

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/viant/embedius/indexer/fs/splitter"
	"github.com/viant/embedius/vectordb/sqlitevec"
)

// Index indexes dataset roots into SQLite and optional upstream sync.
func (s *Service) Index(ctx context.Context, req IndexRequest) error {
	if len(req.Roots) == 0 {
		return fmt.Errorf("no roots specified")
	}
	emb, err := s.resolveEmbedder(req.Embedder)
	if err != nil {
		return err
	}
	if req.ChunkSize <= 0 {
		req.ChunkSize = 4096
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 64
	}
	if req.UpstreamShadow == "" {
		req.UpstreamShadow = "shadow_vec_docs"
	}
	if req.SyncBatch <= 0 {
		req.SyncBatch = 200
	}

	db, err := s.ensureDB(ctx, req.DBPath, false)
	if err != nil {
		return err
	}
	conn, err := ensureSchemaConn(ctx, db)
	if err != nil {
		return err
	}
	defer conn.Close()

	up := req.Upstream
	ownedUpstream := false
	if up == nil && req.UpstreamDriver != "" && req.UpstreamDSN != "" {
		up, err = sql.Open(req.UpstreamDriver, req.UpstreamDSN)
		if err != nil {
			return err
		}
		ownedUpstream = true
		defer func() { _ = up.Close() }()
	}

	factory := splitter.NewFactory(req.ChunkSize)
	for _, spec := range req.Roots {
		absRoot, err := filepath.Abs(spec.Path)
		if err != nil {
			return fmt.Errorf("resolve root path: %w", err)
		}
		if err := ensureDataset(ctx, conn, spec.Name, absRoot); err != nil {
			return err
		}
		if up != nil {
			if err := sqlitevec.SyncUpstream(ctx, db, up, sqlitevec.SyncConfig{
				DatasetID:      spec.Name,
				UpstreamShadow: req.UpstreamShadow,
				LocalShadow:    "_vec_emb_docs",
				AssetTable:     "emb_asset",
				BatchSize:      req.SyncBatch,
				Invalidate:     false,
				Logf:           req.Logf,
				Filter:         newSyncFilter(spec),
			}); err != nil {
				return err
			}
		}

		matcher := newMatcher(spec)
		files, err := listFiles(absRoot, matcher)
		if err != nil {
			return err
		}
		sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
		total := len(files)

		assets, err := loadAssets(ctx, conn, spec.Name)
		if err != nil {
			return err
		}
		seen := make(map[string]bool, len(files))

		tokenTotal := 0
		for i, f := range files {
			seen[f.assetID] = true
			if req.Progress != nil {
				req.Progress(spec.Name, i+1, total, f.rel, tokenTotal)
			}
			info, ok := assets[f.assetID]
			if ok && info.md5 == f.md5 && !info.archived {
				continue
			}
			scn, err := nextSCN(ctx, conn, spec.Name)
			if err != nil {
				return err
			}
			if err := upsertAsset(ctx, conn, spec.Name, f, scn); err != nil {
				return err
			}

			docs, err := splitFile(f.rel, f.data, factory)
			if err != nil {
				return err
			}
			tokens, err := upsertDocuments(ctx, conn, spec.Name, f.assetID, docs, emb, req.BatchSize, scn, f.md5, f.rel, req.Model)
			if err != nil {
				return err
			}
			tokenTotal += tokens
		}

		for assetID, info := range assets {
			if seen[assetID] || info.archived {
				continue
			}
			scn, err := nextSCN(ctx, conn, spec.Name)
			if err != nil {
				return err
			}
			if err := archiveAsset(ctx, conn, spec.Name, assetID, scn); err != nil {
				return err
			}
		}

		if req.Prune {
			if err := pruneArchived(ctx, conn, spec.Name); err != nil {
				return err
			}
		}
		if req.Progress != nil && total == 0 {
			req.Progress(spec.Name, 0, 0, "", tokenTotal)
		}
	}

	if ownedUpstream && up != nil {
		_ = up.Close()
	}
	return nil
}
