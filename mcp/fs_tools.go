package mcp

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

const defaultReadMaxBytes = 8192

func (h *Handler) resolveRootSpec(rootID, rootURI string) (string, error) {
	if h == nil || len(h.rootSpecs) == 0 {
		return "", fmt.Errorf("mcp: roots not configured")
	}
	key := strings.TrimSpace(rootID)
	if key == "" {
		key = strings.TrimSpace(rootURI)
	}
	if key == "" {
		return "", fmt.Errorf("mcp: missing root")
	}
	spec, ok := h.rootSpecs[key]
	if !ok || strings.TrimSpace(spec.Path) == "" {
		return "", fmt.Errorf("mcp: root %q not found", key)
	}
	return spec.Path, nil
}

func safeJoin(root, sub string) (string, string, error) {
	if root == "" {
		return "", "", errors.New("root path is empty")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", "", err
	}
	target := rootAbs
	if strings.TrimSpace(sub) != "" {
		target = filepath.Join(rootAbs, sub)
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return "", "", err
	}
	rootAbs = filepath.Clean(rootAbs)
	targetAbs = filepath.Clean(targetAbs)
	if targetAbs != rootAbs && !strings.HasPrefix(targetAbs, rootAbs+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("path escapes root")
	}
	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil {
		return "", "", err
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		rel = ""
	}
	return rootAbs, targetAbs, nil
}

func normalizeGlobs(patterns []string) []string {
	out := make([]string, 0, len(patterns))
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func globMatch(pattern, value string) bool {
	if pattern == "" || value == "" {
		return false
	}
	if strings.Contains(pattern, "**") {
		return globStarMatch(pattern, value)
	}
	ok, err := path.Match(pattern, value)
	return err == nil && ok
}

func globStarMatch(pattern, value string) bool {
	pattern = strings.TrimSpace(strings.ReplaceAll(pattern, "\\", "/"))
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	if pattern == "" || value == "" {
		return false
	}
	pattern = strings.TrimPrefix(pattern, "./")
	pattern = strings.TrimPrefix(pattern, "/")
	pattern = strings.TrimSuffix(pattern, "/")
	value = strings.TrimPrefix(value, "./")
	value = strings.TrimPrefix(value, "/")
	value = strings.TrimSuffix(value, "/")
	pParts := strings.Split(pattern, "/")
	vParts := strings.Split(value, "/")
	return matchGlobStarParts(pParts, vParts)
}

func matchGlobStarParts(pParts, vParts []string) bool {
	if len(pParts) == 0 {
		return len(vParts) == 0
	}
	if pParts[0] == "**" {
		for i := 0; i <= len(vParts); i++ {
			if matchGlobStarParts(pParts[1:], vParts[i:]) {
				return true
			}
		}
		return false
	}
	if len(vParts) == 0 {
		return false
	}
	ok, err := path.Match(pParts[0], vParts[0])
	if err != nil || !ok {
		return false
	}
	return matchGlobStarParts(pParts[1:], vParts[1:])
}

func listMatchesFilters(relPath, name string, includes, excludes []string) bool {
	for _, pat := range excludes {
		if listGlobMatch(pat, relPath) || listGlobMatch(pat, name) {
			return false
		}
	}
	if len(includes) == 0 {
		return true
	}
	for _, pat := range includes {
		if listGlobMatch(pat, relPath) || listGlobMatch(pat, name) {
			return true
		}
	}
	return false
}

func listGlobMatch(pattern, value string) bool {
	return globMatch(pattern, value)
}

func isBinary(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	for _, c := range data {
		if c == 0 {
			return true
		}
	}
	return !utf8.Valid(data)
}

func isBinaryContent(data []byte) bool {
	if !utf8.Valid(data) {
		return true
	}
	const maxInspect = 1024
	limit := len(data)
	if limit > maxInspect {
		limit = maxInspect
	}
	control := 0
	for _, b := range data[:limit] {
		if b == 0 {
			return true
		}
		if b < 32 && b != '\n' && b != '\r' && b != '\t' {
			control++
		}
	}
	return control > limit/10
}

func fileURI(abs string) string {
	abs = filepath.ToSlash(abs)
	if strings.HasPrefix(abs, "/") {
		return "file://" + abs
	}
	return "file:///" + abs
}

func searchHash(parts ...string) string {
	h := sha1.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sortListItems(items []ListItem) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Path < items[j].Path
	})
}

func newListItem(rootAbs, abs string, info os.FileInfo, rootID string) ListItem {
	rel, _ := filepath.Rel(rootAbs, abs)
	rel = filepath.ToSlash(rel)
	if rel == "." {
		rel = ""
	}
	name := info.Name()
	size := info.Size()
	if info.IsDir() {
		size = 0
	}
	return ListItem{
		URI:      fileURI(abs),
		Path:     rel,
		Name:     name,
		Size:     size,
		Modified: info.ModTime().UTC(),
		RootID:   rootID,
	}
}
