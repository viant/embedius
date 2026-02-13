package service

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// ParseCSV splits comma-separated patterns into a slice.
func ParseCSV(s string) []string {
	return splitCSV(s)
}

// ResolveRoots resolves root specs and optional config DB.
func ResolveRoots(req ResolveRootsRequest) ([]RootSpec, string, error) {
	if req.All && req.ConfigPath == "" {
		return nil, "", fmt.Errorf("--all requires --config or ~/embedius/config.yaml")
	}
	if req.ConfigPath != "" {
		cfg, err := LoadConfig(req.ConfigPath)
		if err != nil {
			return nil, "", err
		}
		var out []RootSpec
		if req.All {
			for name, p := range cfg.Roots {
				if strings.TrimSpace(name) == "" || strings.TrimSpace(p.Path) == "" {
					continue
				}
				out = append(out, RootSpec{Name: name, Path: p.Path, Include: p.Include, Exclude: p.Exclude, MaxSizeBytes: p.MaxSizeBytes})
			}
			sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
			if len(out) == 0 {
				return nil, "", fmt.Errorf("config has no roots")
			}
			return out, cfg.DB, nil
		}
		if req.Root == "" {
			return nil, "", fmt.Errorf("root is required without --all")
		}
		p, ok := cfg.Roots[req.Root]
		if !ok || strings.TrimSpace(p.Path) == "" {
			return nil, "", fmt.Errorf("root %q not found in config", req.Root)
		}
		return []RootSpec{{Name: req.Root, Path: p.Path, Include: p.Include, Exclude: p.Exclude, MaxSizeBytes: p.MaxSizeBytes}}, cfg.DB, nil
	}
	if req.Root == "" {
		return nil, "", fmt.Errorf("root is required")
	}
	if req.RequirePath && strings.TrimSpace(req.RootPath) == "" {
		return nil, "", fmt.Errorf("path is required")
	}
	spec := RootSpec{Name: req.Root, Path: req.RootPath}
	if len(req.Include) > 0 {
		spec.Include = req.Include
	}
	if len(req.Exclude) > 0 {
		spec.Exclude = req.Exclude
	}
	if req.MaxSizeBytes > 0 {
		spec.MaxSizeBytes = req.MaxSizeBytes
	}
	return []RootSpec{spec}, "", nil
}

// Roots returns summary metadata for roots.
func (s *Service) Roots(ctx context.Context, req RootsRequest) ([]RootInfo, error) {
	db, err := s.ensureDB(ctx, req.DBPath, false)
	if err != nil {
		return nil, err
	}
	if err := ensureSchema(ctx, db, s.driver); err != nil {
		return nil, err
	}
	docsTable := localDocsTable(s.driver)
	query := fmt.Sprintf(`SELECT r.dataset_id, r.source_uri, r.last_scn, r.last_indexed_at,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS assets,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id AND a.archived = 1) AS assets_archived,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id AND a.archived = 0) AS assets_active,
    (SELECT COALESCE(SUM(size), 0) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS assets_size,
    (SELECT MAX(mod_time) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS last_asset_mod_time,
    (SELECT md5 FROM emb_asset a WHERE a.dataset_id = r.dataset_id ORDER BY mod_time DESC LIMIT 1) AS last_asset_md5,
    (SELECT COUNT(*) FROM %s d WHERE d.dataset_id = r.dataset_id) AS docs,
    (SELECT COUNT(*) FROM %s d WHERE d.dataset_id = r.dataset_id AND d.archived = 1) AS docs_archived,
    (SELECT COUNT(*) FROM %s d WHERE d.dataset_id = r.dataset_id AND d.archived = 0) AS docs_active,
    (SELECT COALESCE(AVG(LENGTH(content)), 0) FROM %s d WHERE d.dataset_id = r.dataset_id AND d.archived = 0) AS avg_doc_len,
    (SELECT MAX(scn) FROM %s d WHERE d.dataset_id = r.dataset_id) AS last_doc_scn,
    (SELECT embedding_model FROM %s d WHERE d.dataset_id = r.dataset_id AND embedding_model <> '' ORDER BY scn DESC LIMIT 1) AS embedding_model,
    (SELECT COALESCE(MAX(last_scn), 0) FROM vec_sync_state s WHERE s.dataset_id = r.dataset_id) AS last_sync_scn,
    (SELECT GROUP_CONCAT(DISTINCT shadow_table) FROM vec_sync_state s WHERE s.dataset_id = r.dataset_id) AS upstream_shadow
FROM emb_root r`, docsTable, docsTable, docsTable, docsTable, docsTable, docsTable)
	var rows *sql.Rows
	withSyncState := true
	if req.Root != "" {
		query += " WHERE r.dataset_id = ?"
		rows, err = db.QueryContext(ctx, query, req.Root)
	} else {
		query += " ORDER BY r.dataset_id"
		rows, err = db.QueryContext(ctx, query)
	}
	if err != nil {
		if isMissingTableErr(err, "vec_sync_state") {
			withSyncState = false
			query = fmt.Sprintf(`SELECT r.dataset_id, r.source_uri, r.last_scn, r.last_indexed_at,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS assets,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id AND a.archived = 1) AS assets_archived,
    (SELECT COUNT(*) FROM emb_asset a WHERE a.dataset_id = r.dataset_id AND a.archived = 0) AS assets_active,
    (SELECT COALESCE(SUM(size), 0) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS assets_size,
    (SELECT MAX(mod_time) FROM emb_asset a WHERE a.dataset_id = r.dataset_id) AS last_asset_mod_time,
    (SELECT md5 FROM emb_asset a WHERE a.dataset_id = r.dataset_id ORDER BY mod_time DESC LIMIT 1) AS last_asset_md5,
    (SELECT COUNT(*) FROM %s d WHERE d.dataset_id = r.dataset_id) AS docs,
    (SELECT COUNT(*) FROM %s d WHERE d.dataset_id = r.dataset_id AND d.archived = 1) AS docs_archived,
    (SELECT COUNT(*) FROM %s d WHERE d.dataset_id = r.dataset_id AND d.archived = 0) AS docs_active,
    (SELECT COALESCE(AVG(LENGTH(content)), 0) FROM %s d WHERE d.dataset_id = r.dataset_id AND d.archived = 0) AS avg_doc_len,
    (SELECT MAX(scn) FROM %s d WHERE d.dataset_id = r.dataset_id) AS last_doc_scn,
    (SELECT embedding_model FROM %s d WHERE d.dataset_id = r.dataset_id AND embedding_model <> '' ORDER BY scn DESC LIMIT 1) AS embedding_model
FROM emb_root r`, docsTable, docsTable, docsTable, docsTable, docsTable, docsTable)
			if req.Root != "" {
				query += " WHERE r.dataset_id = ?"
				rows, err = db.QueryContext(ctx, query, req.Root)
			} else {
				query += " ORDER BY r.dataset_id"
				rows, err = db.QueryContext(ctx, query)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	var out []RootInfo
	for rows.Next() {
		var info RootInfo
		if withSyncState {
			if err := rows.Scan(&info.DatasetID, &info.SourceURI, &info.LastSCN, &info.LastIndexedAt, &info.Assets, &info.AssetsArchived, &info.AssetsActive, &info.AssetsSize, &info.LastAssetMod, &info.LastAssetMD5, &info.Documents, &info.DocsArchived, &info.DocsActive, &info.AvgDocLen, &info.LastDocSCN, &info.EmbeddingModel, &info.LastSyncSCN, &info.UpstreamShadow); err != nil {
				return nil, err
			}
		} else {
			if err := rows.Scan(&info.DatasetID, &info.SourceURI, &info.LastSCN, &info.LastIndexedAt, &info.Assets, &info.AssetsArchived, &info.AssetsActive, &info.AssetsSize, &info.LastAssetMod, &info.LastAssetMD5, &info.Documents, &info.DocsArchived, &info.DocsActive, &info.AvgDocLen, &info.LastDocSCN, &info.EmbeddingModel); err != nil {
				return nil, err
			}
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
