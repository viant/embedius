package mcp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (h *Handler) list(ctx context.Context, in *ListInput) (*ListOutput, error) {
	if h == nil || h.service == nil {
		return nil, fmt.Errorf("mcp: service unavailable")
	}
	if in == nil {
		in = &ListInput{}
	}
	rootPath, err := h.resolveRootSpec(in.RootID, in.RootURI)
	if err != nil {
		return nil, err
	}
	rootAbs, targetAbs, _, err := resolveRootTarget(rootPath, in.Path)
	if err != nil {
		return nil, err
	}
	maxItems := in.MaxItems
	if maxItems < 0 {
		maxItems = 0
	}
	include := normalizeGlobs(in.Include)
	exclude := normalizeGlobs(in.Exclude)

	var items []ListItem
	if in.Recursive {
		err = filepath.WalkDir(targetAbs, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(rootAbs, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if rel == "." {
				rel = ""
			}
			if rel != "" && !listMatchesFilters(rel, info.Name(), include, exclude) {
				return nil
			}
			if rel == "" {
				return nil
			}
			items = append(items, newListItem(rootAbs, path, info, in.RootID))
			if maxItems > 0 && len(items) >= maxItems {
				return errStopWalk
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStopWalk) {
			return nil, err
		}
	} else {
		entries, err := os.ReadDir(targetAbs)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			full := filepath.Join(targetAbs, entry.Name())
			rel, err := filepath.Rel(rootAbs, full)
			if err != nil {
				return nil, err
			}
			rel = filepath.ToSlash(rel)
			if rel == "." {
				rel = ""
			}
			if rel == "" {
				continue
			}
			if !listMatchesFilters(rel, info.Name(), include, exclude) {
				continue
			}
			items = append(items, newListItem(rootAbs, full, info, in.RootID))
			if maxItems > 0 && len(items) >= maxItems {
				break
			}
		}
	}
	sortListItems(items)
	return &ListOutput{Items: items, Total: len(items)}, nil
}

var errStopWalk = errors.New("stop walk")

func resolveRootTarget(rootPath, subPath string) (string, string, string, error) {
	rootAbs, targetAbs, err := func() (string, string, error) {
		rootAbs, targetAbs, err := safeJoin(rootPath, subPath)
		if err != nil {
			return "", "", err
		}
		return rootAbs, targetAbs, nil
	}()
	if err != nil {
		return "", "", "", err
	}
	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil {
		return "", "", "", err
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		rel = ""
	}
	return rootAbs, targetAbs, rel, nil
}
