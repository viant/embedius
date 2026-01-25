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
		if err := upsertRootConfig(ctx, conn, spec.Name, encodeGlobList(spec.Include), encodeGlobList(spec.Exclude), spec.MaxSizeBytes); err != nil {
			return err
		}
		if req.Logf != nil {
			if info, err := rootSummary(ctx, conn, spec.Name); err == nil {
				logRootSummary(req.Logf, "index start", info)
			}
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
			if req.Logf != nil {
				req.Logf("index file root=%s path=%s size=%d md5=%s", spec.Name, f.rel, f.size, f.md5)
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
		if req.Logf != nil {
			if info, err := rootSummary(ctx, conn, spec.Name); err == nil {
				logRootSummary(req.Logf, "index done", info)
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

func rootSummary(ctx context.Context, q sqlQueryer, datasetID string) (RootInfo, error) {
	query := `SELECT r.dataset_id, r.source_uri, r.last_scn, r.last_indexed_at,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS assets,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id AND a.archived = 1) AS assets_archived,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id AND a.archived = 0) AS assets_active,
    (SELECT COALESCE(SUM(size), 0) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS assets_size,
    (SELECT MAX(mod_time) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS last_asset_mod_time,
    (SELECT md5 FROM emb_asset a WHERE a.dataset_id = r.dataset_id ORDER BY mod_time DESC LIMIT 1) AS last_asset_md5,
    (SELECT COUNT(*) FROM _vec_emb_docs d WHERE d.dataset_id = r.dataset_id) AS docs,
    (SELECT COUNT(*) FROM _vec_emb_docs d WHERE d.dataset_id = r.dataset_id AND d.archived = 1) AS docs_archived,
    (SELECT COUNT(*) FROM _vec_emb_docs d WHERE d.dataset_id = r.dataset_id AND d.archived = 0) AS docs_active,
    (SELECT COALESCE(AVG(LENGTH(content)), 0) FROM _vec_emb_docs d WHERE d.dataset_id = r.dataset_id AND d.archived = 0) AS avg_doc_len,
    (SELECT MAX(scn) FROM _vec_emb_docs d WHERE d.dataset_id = r.dataset_id) AS last_doc_scn,
    (SELECT embedding_model FROM _vec_emb_docs d WHERE d.dataset_id = r.dataset_id AND embedding_model <> '' ORDER BY scn DESC LIMIT 1) AS embedding_model,
    (SELECT COALESCE(MAX(last_scn), 0) FROM vec_sync_state s WHERE s.dataset_id = r.dataset_id) AS last_sync_scn,
    (SELECT GROUP_CONCAT(DISTINCT shadow_table) FROM vec_sync_state s WHERE s.dataset_id = r.dataset_id) AS upstream_shadow
FROM emb_root r WHERE r.dataset_id = ?`
	var info RootInfo
	row := q.QueryRowContext(ctx, query, datasetID)
	if err := row.Scan(&info.DatasetID, &info.SourceURI, &info.LastSCN, &info.LastIndexedAt, &info.Assets, &info.AssetsArchived, &info.AssetsActive, &info.AssetsSize, &info.LastAssetMod, &info.LastAssetMD5, &info.Documents, &info.DocsArchived, &info.DocsActive, &info.AvgDocLen, &info.LastDocSCN, &info.EmbeddingModel, &info.LastSyncSCN, &info.UpstreamShadow); err != nil {
		return RootInfo{}, err
	}
	return info, nil
}

func logRootSummary(logf func(format string, args ...any), prefix string, info RootInfo) {
	if logf == nil {
		return
	}
	lastIdx := ""
	if info.LastIndexedAt.Valid {
		lastIdx = info.LastIndexedAt.String
	}
	lastDocSCN := int64(0)
	if info.LastDocSCN.Valid {
		lastDocSCN = info.LastDocSCN.Int64
	}
	logf("%s root=%s scn=%d last_doc_scn=%d last_sync_scn=%d assets=%d docs=%d indexed_at=%s", prefix, info.DatasetID, info.LastSCN, lastDocSCN, info.LastSyncSCN, info.Assets, info.Documents, lastIdx)
}
