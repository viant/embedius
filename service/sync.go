package service

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/viant/embedius/vectordb/sqlitevec"
)

// Sync pulls upstream changes into the local SQLite store.
func (s *Service) Sync(ctx context.Context, req SyncRequest) error {
	if len(req.Roots) == 0 {
		return fmt.Errorf("no roots specified")
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
	if err := ensureSchema(ctx, db); err != nil {
		return err
	}

	up := req.Upstream
	ownedUpstream := false
	if up == nil {
		if req.UpstreamDriver == "" || req.UpstreamDSN == "" {
			return fmt.Errorf("upstream driver/dsn required")
		}
		up, err = sql.Open(req.UpstreamDriver, req.UpstreamDSN)
		if err != nil {
			return err
		}
		ownedUpstream = true
		defer func() { _ = up.Close() }()
	}

	for _, spec := range req.Roots {
		if req.Logf != nil {
			req.Logf("sync root=%s starting", spec.Name)
		}
		filter := newSyncFilter(spec)
		if err := sqlitevec.SyncUpstream(ctx, db, up, sqlitevec.SyncConfig{
			DatasetID:      spec.Name,
			UpstreamShadow: req.UpstreamShadow,
			LocalShadow:    "_vec_emb_docs",
			AssetTable:     "emb_asset",
			BatchSize:      req.SyncBatch,
			Invalidate:     req.Invalidate,
			Logf:           req.Logf,
			Filter:         filter,
		}); err != nil {
			return err
		}
	}
	if ownedUpstream && up != nil {
		_ = up.Close()
	}
	return nil
}
