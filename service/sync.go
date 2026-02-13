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
	driver := s.driver
	if driver != "sqlite" {
		return fmt.Errorf("sync: upstream pull requires sqlite store")
	}
	if err := ensureSchema(ctx, db, driver); err != nil {
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
		if err := ensureDataset(ctx, db, spec.Name, spec.Path, driver); err != nil {
			return err
		}
		if err := upsertRootConfig(ctx, db, spec.Name, encodeGlobList(spec.Include), encodeGlobList(spec.Exclude), spec.MaxSizeBytes, driver); err != nil {
			return err
		}
		filter := newSyncFilter(spec)
		if req.ForceReset {
			if req.Logf != nil {
				req.Logf("sync root=%s force reset local dataset", spec.Name)
			}
			if err := sqlitevec.ResetLocalDataset(ctx, db, sqlitevec.SyncConfig{
				DatasetID:      spec.Name,
				UpstreamShadow: req.UpstreamShadow,
				LocalShadow:    "_vec_emb_docs",
				AssetTable:     "emb_asset",
			}); err != nil {
				return err
			}
		}
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
		if err := syncRootConfig(ctx, db, up, spec.Name, req.Logf, driver); err != nil {
			return err
		}
	}
	if ownedUpstream && up != nil {
		_ = up.Close()
	}
	return nil
}

func syncRootConfig(ctx context.Context, local *sql.DB, upstream *sql.DB, datasetID string, logf func(format string, args ...any), driver string) error {
	row := upstream.QueryRowContext(ctx, `SELECT include_globs, exclude_globs, max_size_bytes FROM emb_root_config WHERE dataset_id = ?`, datasetID)
	var include sql.NullString
	var exclude sql.NullString
	var maxSize sql.NullInt64
	if err := row.Scan(&include, &exclude, &maxSize); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	if err := upsertRootConfig(ctx, local, datasetID, include.String, exclude.String, maxSize.Int64, driver); err != nil {
		return err
	}
	if logf != nil {
		logf("sync root=%s config updated", datasetID)
	}
	return nil
}
