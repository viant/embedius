package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/viant/embedius/matching"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/service"
)

func (h *Handler) match(ctx context.Context, in *MatchInput) (*MatchOutput, error) {
	if h == nil || h.service == nil {
		return nil, fmt.Errorf("mcp: service unavailable")
	}
	if in == nil {
		in = &MatchInput{}
	}
	if strings.TrimSpace(in.Query) == "" {
		return nil, fmt.Errorf("mcp: missing query")
	}
	rootIDs := resolveMatchRoots(h, in)
	if len(rootIDs) == 0 {
		return nil, fmt.Errorf("mcp: no roots available")
	}
	limit := in.MaxDocuments
	if limit <= 0 {
		limit = 10
	}
	model := h.model
	if strings.TrimSpace(in.Model) != "" {
		model = in.Model
	}
	qvec, _, err := h.queryEmbedding(ctx, in.Query, model)
	if err != nil {
		return nil, err
	}
	var matcher *matching.Manager
	if in.Match != nil {
		opts := in.Match.Options()
		if len(opts) > 0 {
			matcher = matching.New(opts...)
		}
	}
	results := make([]schema.Document, 0, limit)
	for _, root := range rootIDs {
		items, err := h.service.SearchWithEmbedding(ctx, service.SearchRequest{
			DBPath:   h.dbPath,
			Dataset:  root,
			Query:    in.Query,
			Embedder: h.embedder,
			Model:    model,
			Limit:    limit,
		}, qvec)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if in.Path != "" && !strings.HasPrefix(item.Path, in.Path) {
				continue
			}
			if matcher != nil && matcher.IsExcluded(item.Path, 0) {
				continue
			}
			content := item.Content
			if in.IncludeFile {
				if spec, ok := h.rootSpecs[root]; ok && strings.TrimSpace(spec.Path) != "" && strings.TrimSpace(item.Path) != "" {
					if _, fullPath, err := safeJoin(spec.Path, item.Path); err == nil {
						if data, err := os.ReadFile(fullPath); err == nil {
							content = string(data)
						}
					}
				}
			}
			meta := map[string]interface{}{}
			if strings.TrimSpace(item.Meta) != "" {
				_ = json.Unmarshal([]byte(item.Meta), &meta)
			}
			meta["path"] = item.Path
			meta["docId"] = item.ID
			meta["rootId"] = root
			meta["score"] = item.Score
			doc := schema.Document{
				PageContent: content,
				Metadata:    meta,
				Score:       float32(item.Score),
			}
			results = append(results, doc)
			if len(results) >= limit {
				break
			}
		}
		if len(results) >= limit {
			break
		}
	}
	limitBytes := effectiveLimitBytes(in.LimitBytes)
	cursor := effectiveCursor(in.Cursor)
	pageDocs, hasNext := selectDocPage(results, limitBytes, cursor)
	if totalFormattedBytes(results) <= limitBytes {
		hasNext = false
	}
	content := buildDocumentContent(pageDocs)
	output := &MatchOutput{
		Content:       content,
		Documents:     pageDocs,
		DocumentsSize: documentsSize(pageDocs),
		Cursor:        cursor,
		LimitBytes:    limitBytes,
		DocumentRoots: buildDocumentRootsMap(pageDocs),
	}
	if hasNext {
		output.NextCursor = cursor + 1
	}
	return output, nil
}

func resolveMatchRoots(h *Handler, in *MatchInput) []string {
	var roots []string
	if in == nil {
		return nil
	}
	if len(in.RootIDs) > 0 {
		roots = append(roots, in.RootIDs...)
	} else if len(in.RootURI) > 0 {
		roots = append(roots, in.RootURI...)
	} else if len(in.Roots) > 0 {
		roots = append(roots, in.Roots...)
	} else {
		for name := range h.rootSpecs {
			roots = append(roots, name)
		}
	}
	out := make([]string, 0, len(roots))
	seen := map[string]bool{}
	for _, r := range roots {
		r = strings.TrimSpace(r)
		if r == "" || seen[r] {
			continue
		}
		if _, ok := h.rootSpecs[r]; !ok {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

func effectiveLimitBytes(limit int) int {
	if limit <= 0 {
		return 7000
	}
	if limit > 200000 {
		return 200000
	}
	return limit
}

func effectiveCursor(cursor int) int {
	if cursor <= 0 {
		return 1
	}
	return cursor
}

func selectDocPage(docs []schema.Document, limitBytes int, cursor int) ([]schema.Document, bool) {
	if limitBytes <= 0 || len(docs) == 0 {
		return nil, false
	}
	pages := make([][]schema.Document, 0, 4)
	var cur []schema.Document
	used := 0
	for _, d := range docs {
		loc := documentLocation(d)
		formatted := formatDocument(loc, d.PageContent)
		fragBytes := len(formatted)
		if fragBytes > limitBytes {
			if len(cur) > 0 {
				pages = append(pages, cur)
				cur = nil
				used = 0
			}
			pages = append(pages, []schema.Document{d})
			continue
		}
		if used+fragBytes > limitBytes {
			pages = append(pages, cur)
			cur = nil
			used = 0
		}
		cur = append(cur, d)
		used += fragBytes
	}
	if len(cur) > 0 {
		pages = append(pages, cur)
	}
	if len(pages) == 0 {
		return nil, false
	}
	if cursor < 1 {
		cursor = 1
	}
	if cursor > len(pages) {
		cursor = len(pages)
	}
	sel := pages[cursor-1]
	hasNext := cursor < len(pages)
	return sel, hasNext
}

func formatDocument(loc string, content string) string {
	ext := strings.Trim(path.Ext(loc), ".")
	return fmt.Sprintf("file: %v\n```%v\n%v\n````\n\n", loc, ext, content)
}

func documentsSize(docs []schema.Document) int {
	total := 0
	for _, d := range docs {
		total += len(d.PageContent)
	}
	return total
}

func totalFormattedBytes(docs []schema.Document) int {
	total := 0
	for _, d := range docs {
		loc := documentLocation(d)
		total += len(formatDocument(loc, d.PageContent))
	}
	return total
}

func buildDocumentContent(docs []schema.Document) string {
	if len(docs) == 0 {
		return ""
	}
	var b strings.Builder
	for _, doc := range docs {
		loc := documentLocation(doc)
		_, _ = b.WriteString(formatDocument(loc, doc.PageContent))
	}
	return b.String()
}

func documentLocation(doc schema.Document) string {
	loc := ""
	if doc.Metadata != nil {
		if v, ok := doc.Metadata["path"]; ok {
			if s, _ := v.(string); s != "" {
				loc = s
			}
		}
		if loc == "" {
			if v, ok := doc.Metadata["docId"]; ok {
				if s, _ := v.(string); s != "" {
					loc = s
				}
			}
		}
	}
	return loc
}

func buildDocumentRootsMap(docs []schema.Document) map[string]string {
	if len(docs) == 0 {
		return nil
	}
	roots := make(map[string]string)
	for _, doc := range docs {
		path := ""
		rootID := ""
		if doc.Metadata != nil {
			if v, ok := doc.Metadata["path"]; ok {
				if s, _ := v.(string); strings.TrimSpace(s) != "" {
					path = s
				}
			}
			if v, ok := doc.Metadata["rootId"]; ok {
				if s, _ := v.(string); strings.TrimSpace(s) != "" {
					rootID = s
				}
			}
		}
		if path == "" || rootID == "" {
			continue
		}
		roots[path] = rootID
	}
	if len(roots) == 0 {
		return nil
	}
	return roots
}
