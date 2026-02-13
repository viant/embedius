package service

import (
	"context"
	"fmt"
	"strings"
)

// Admin performs maintenance tasks.
func (s *Service) Admin(ctx context.Context, req AdminRequest) ([]AdminResult, error) {
	if len(req.Roots) == 0 {
		return nil, fmt.Errorf("no roots specified")
	}
	if req.Action == "" {
		req.Action = "rebuild"
	}
	if req.Shadow == "" {
		req.Shadow = "main._vec_emb_docs"
	}
	if req.SyncShadow == "" {
		req.SyncShadow = "shadow_vec_docs"
	}
	db, err := s.ensureDB(ctx, req.DBPath, true)
	if err != nil {
		return nil, err
	}
	conn, err := ensureSchemaConn(ctx, db, s.driver)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if s.driver != "sqlite" && (req.Action == "rebuild" || req.Action == "invalidate") {
		return nil, fmt.Errorf("admin: action %q requires sqlite store", req.Action)
	}
	if s.driver == "sqlite" {
		if _, err := conn.ExecContext(ctx, `CREATE VIRTUAL TABLE IF NOT EXISTS vec_admin USING vec_admin(op)`); err != nil {
			return nil, err
		}
	}

	var results []AdminResult
	for _, spec := range req.Roots {
		switch req.Action {
		case "rebuild":
			target := req.Shadow
			if !strings.Contains(target, ".") {
				target = "main." + target
			}
			q := fmt.Sprintf("%s:%s", target, spec.Name)
			var op string
			if err := conn.QueryRowContext(ctx, `SELECT op FROM vec_admin WHERE op MATCH ?`, q).Scan(&op); err != nil {
				if strings.Contains(err.Error(), "xBestIndex malfunction") || strings.Contains(err.Error(), "no such module: vec_admin") {
					results = append(results, AdminResult{Root: spec.Name, Action: req.Action, Details: "skipped (vec_admin unavailable)"})
					continue
				}
				return nil, err
			}
			results = append(results, AdminResult{Root: spec.Name, Action: req.Action, Details: op})
		case "invalidate":
			target := req.Shadow
			if !strings.Contains(target, ".") {
				target = "main." + target
			}
			if _, err := conn.ExecContext(ctx, `SELECT vec_invalidate(?, ?)`, target, spec.Name); err != nil {
				return nil, err
			}
			results = append(results, AdminResult{Root: spec.Name, Action: req.Action, Details: target})
		case "prune":
			targetSCN := req.PruneSCN
			if targetSCN == 0 {
				lastSCN, err := pruneMaxSCN(ctx, conn, spec.Name, req.SyncShadow)
				if err != nil {
					return nil, err
				}
				if lastSCN == 0 && !req.Force {
					results = append(results, AdminResult{Root: spec.Name, Action: req.Action, Details: "skipped (last_scn=0)"})
					continue
				}
				targetSCN = lastSCN
			} else if !req.Force {
				return nil, fmt.Errorf("prune %s: --scn requires --force", spec.Name)
			}
			if err := pruneArchivedBefore(ctx, conn, spec.Name, targetSCN, s.driver); err != nil {
				return nil, err
			}
			results = append(results, AdminResult{Root: spec.Name, Action: req.Action, Details: fmt.Sprintf("up_to_scn=%d", targetSCN)})
		case "check":
			stats, err := checkIntegrity(ctx, conn, spec.Name, s.driver)
			if err != nil {
				return nil, err
			}
			results = append(results, AdminResult{Root: spec.Name, Action: req.Action, Stats: stats})
		default:
			return nil, fmt.Errorf("unknown action: %s", req.Action)
		}
	}
	return results, nil
}
