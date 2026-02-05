package mcp

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"strings"
)

func (h *Handler) grepFiles(ctx context.Context, in *GrepInput) (*GrepOutput, error) {
	if h == nil || h.service == nil {
		return nil, fmt.Errorf("mcp: service unavailable")
	}
	if in == nil {
		in = &GrepInput{}
	}
	pattern := strings.TrimSpace(in.Pattern)
	if pattern == "" {
		return nil, fmt.Errorf("pattern must not be empty")
	}
	rootPath, err := h.resolveRootSpec(in.RootID, in.RootURI)
	if err != nil {
		return nil, err
	}
	rootAbs, targetAbs, relBase, err := resolveRootTarget(rootPath, in.Path)
	if err != nil {
		return nil, err
	}
	mode := strings.ToLower(strings.TrimSpace(in.Mode))
	if mode == "" {
		mode = "match"
	}
	limitBytes := in.Bytes
	if limitBytes <= 0 {
		limitBytes = 512
	}
	limitLines := in.Lines
	if limitLines <= 0 {
		limitLines = 32
	}
	maxFiles := in.MaxFiles
	if maxFiles <= 0 {
		maxFiles = 20
	}
	maxBlocks := in.MaxBlocks
	if maxBlocks <= 0 {
		maxBlocks = 200
	}
	maxSize := in.MaxSize
	if maxSize <= 0 {
		maxSize = 1024 * 1024
	}
	skipBinary := in.SkipBinary
	if !in.SkipBinary {
		skipBinary = true
	}

	patList := splitPatterns(pattern)
	exclList := splitPatterns(strings.TrimSpace(in.ExcludePattern))
	if len(patList) == 0 {
		return nil, fmt.Errorf("pattern must not be empty")
	}
	matchers, err := compilePatterns(patList, in.CaseInsensitive)
	if err != nil {
		return nil, err
	}
	excludeMatchers, err := compilePatterns(exclList, in.CaseInsensitive)
	if err != nil {
		return nil, err
	}

	includes := normalizeGlobs(in.Include)
	excludes := normalizeGlobs(in.Exclude)

	stats := GrepStats{}
	var files []GrepFile
	totalBlocks := 0

	if info, err := os.Stat(targetAbs); err == nil && !info.IsDir() {
		return grepSingleFile(targetAbs, rootAbs, relBase, in, matchers, excludeMatchers, includes, excludes, mode, limitBytes, limitLines, maxSize, skipBinary)
	}

	walkErr := filepath.WalkDir(targetAbs, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != targetAbs && !in.Recursive {
				return filepath.SkipDir
			}
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
			return nil
		}
		if !listMatchesFilters(rel, info.Name(), includes, excludes) {
			return nil
		}
		stats.Scanned++
		if stats.Matched >= maxFiles || totalBlocks >= maxBlocks {
			stats.Truncated = true
			return errStopWalk
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		if maxSize > 0 && len(data) > maxSize {
			data = data[:maxSize]
		}
		if skipBinary && isBinary(data) {
			return nil
		}
		text := string(data)
		lines := strings.Split(text, "\n")
		var matchLines []int
		for i, line := range lines {
			if lineMatches(line, matchers, excludeMatchers) {
				matchLines = append(matchLines, i)
			}
		}
		if len(matchLines) == 0 {
			return nil
		}
		stats.Matched++
		gf := GrepFile{Path: rel, URI: fileURI(path), Matches: len(matchLines)}
		gf.SearchHash = grepSearchHash(in, rootAbs)
		if mode == "head" {
			end := limitLines
			if end > len(lines) {
				end = len(lines)
			}
			snippetText := joinLines(lines[:end])
			if len(snippetText) > limitBytes {
				snippetText = snippetText[:limitBytes]
			}
			gf.Snippets = append(gf.Snippets, GrepSnippet{StartLine: 1, EndLine: end, Text: snippetText})
			gf.RangeKey = fmt.Sprintf("%d-%d", 1, end)
			files = append(files, gf)
			return nil
		}
		for _, idx := range matchLines {
			if totalBlocks >= maxBlocks {
				stats.Truncated = true
				break
			}
			start := idx - limitLines/2
			if start < 0 {
				start = 0
			}
			end := start + limitLines
			if end > len(lines) {
				end = len(lines)
			}
			snippetText := joinLines(lines[start:end])
			cut := false
			if len(snippetText) > limitBytes {
				snippetText = snippetText[:limitBytes]
				cut = true
			}
			gf.Snippets = append(gf.Snippets, GrepSnippet{
				StartLine:   start + 1,
				EndLine:     end,
				Text:        snippetText,
				OffsetBytes: 0,
				LengthBytes: len(snippetText),
				Cut:         cut,
			})
			if gf.RangeKey == "" {
				gf.RangeKey = fmt.Sprintf("%d-%d", start+1, end)
			}
			totalBlocks++
			if totalBlocks >= maxBlocks {
				stats.Truncated = true
				break
			}
		}
		files = append(files, gf)
		if stats.Matched >= maxFiles || totalBlocks >= maxBlocks {
			stats.Truncated = true
			return errStopWalk
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, errStopWalk) {
		return nil, walkErr
	}
	return &GrepOutput{Stats: stats, Files: files}, nil
}

func grepSingleFile(path string, rootAbs string, relBase string, input *GrepInput, matchers, excludeMatchers []*regexp.Regexp, includes, excludes []string, mode string, limitBytes, limitLines, maxSize int, skipBinary bool) (*GrepOutput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if maxSize > 0 && len(data) > maxSize {
		data = data[:maxSize]
	}
	if skipBinary && isBinary(data) {
		return &GrepOutput{Stats: GrepStats{}}, nil
	}
	text := string(data)
	lines := strings.Split(text, "\n")
	var matchLines []int
	for i, line := range lines {
		if lineMatches(line, matchers, excludeMatchers) {
			matchLines = append(matchLines, i)
		}
	}
	if len(matchLines) == 0 {
		return &GrepOutput{Stats: GrepStats{}}, nil
	}
	rel := relativePath(rootAbs, path)
	if rel == "" {
		rel = relBase
	}
	name := pathpkg.Base(strings.TrimSuffix(rel, "/"))
	if !listMatchesFilters(rel, name, includes, excludes) {
		return &GrepOutput{Stats: GrepStats{}}, nil
	}
	stats := GrepStats{Scanned: 1, Matched: 1}
	gf := GrepFile{Path: rel, URI: fileURI(path), Matches: len(matchLines)}
	gf.SearchHash = grepSearchHash(input, rootAbs)
	if mode == "head" {
		end := limitLines
		if end > len(lines) {
			end = len(lines)
		}
		snippetText := joinLines(lines[:end])
		if len(snippetText) > limitBytes {
			snippetText = snippetText[:limitBytes]
		}
		gf.Snippets = append(gf.Snippets, GrepSnippet{StartLine: 1, EndLine: end, Text: snippetText})
		gf.RangeKey = fmt.Sprintf("%d-%d", 1, end)
		return &GrepOutput{Stats: stats, Files: []GrepFile{gf}}, nil
	}
	totalBlocks := 0
	for _, idx := range matchLines {
		start := idx - limitLines/2
		if start < 0 {
			start = 0
		}
		end := start + limitLines
		if end > len(lines) {
			end = len(lines)
		}
		snippetText := joinLines(lines[start:end])
		cut := false
		if len(snippetText) > limitBytes {
			snippetText = snippetText[:limitBytes]
			cut = true
		}
		gf.Snippets = append(gf.Snippets, GrepSnippet{
			StartLine:   start + 1,
			EndLine:     end,
			Text:        snippetText,
			OffsetBytes: 0,
			LengthBytes: len(snippetText),
			Cut:         cut,
		})
		if gf.RangeKey == "" {
			gf.RangeKey = fmt.Sprintf("%d-%d", start+1, end)
		}
		totalBlocks++
	}
	if totalBlocks > 200 {
		stats.Truncated = true
	}
	return &GrepOutput{Stats: stats, Files: []GrepFile{gf}}, nil
}

func splitPatterns(expr string) []string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	lower := strings.ToLower(expr)
	sep := " or "
	var parts []string
	start := 0
	for {
		idx := strings.Index(lower[start:], sep)
		if idx == -1 {
			break
		}
		abs := start + idx
		parts = append(parts, expr[start:abs])
		start = abs + len(sep)
	}
	if start == 0 {
		parts = []string{expr}
	} else {
		parts = append(parts, expr[start:])
	}
	var out []string
	for _, p := range parts {
		for _, sub := range strings.Split(p, "|") {
			if v := strings.TrimSpace(sub); v != "" {
				out = append(out, v)
			}
		}
	}
	return out
}

func compilePatterns(patterns []string, caseInsensitive bool) ([]*regexp.Regexp, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		pat := strings.TrimSpace(p)
		if pat == "" {
			continue
		}
		if caseInsensitive {
			pat = "(?i)" + pat
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			literal := regexp.QuoteMeta(strings.TrimSpace(p))
			if caseInsensitive {
				literal = "(?i)" + literal
			}
			re, err = regexp.Compile(literal)
			if err != nil {
				return nil, fmt.Errorf("invalid pattern %q: %w", p, err)
			}
		}
		out = append(out, re)
	}
	return out, nil
}

func lineMatches(line string, includes, excludes []*regexp.Regexp) bool {
	matched := false
	if len(includes) == 0 {
		matched = true
	} else {
		for _, re := range includes {
			if re.FindStringIndex(line) != nil {
				matched = true
				break
			}
		}
	}
	if !matched {
		return false
	}
	for _, re := range excludes {
		if re.FindStringIndex(line) != nil {
			return false
		}
	}
	return true
}

func joinLines(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	var b strings.Builder
	for i, ln := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(ln)
	}
	return b.String()
}

type grepSearchHashInput struct {
	Pattern         string   `json:"pattern,omitempty"`
	ExcludePattern  string   `json:"excludePattern,omitempty"`
	Root            string   `json:"root,omitempty"`
	RootID          string   `json:"rootId,omitempty"`
	Path            string   `json:"path,omitempty"`
	Recursive       bool     `json:"recursive,omitempty"`
	Include         []string `json:"include,omitempty"`
	Exclude         []string `json:"exclude,omitempty"`
	CaseInsensitive bool     `json:"caseInsensitive,omitempty"`
	Mode            string   `json:"mode,omitempty"`
	Bytes           int      `json:"bytes,omitempty"`
	Lines           int      `json:"lines,omitempty"`
	MaxFiles        int      `json:"maxFiles,omitempty"`
	MaxBlocks       int      `json:"maxBlocks,omitempty"`
	SkipBinary      bool     `json:"skipBinary,omitempty"`
	MaxSize         int      `json:"maxSize,omitempty"`
	Concurrency     int      `json:"concurrency,omitempty"`
}

func grepSearchHash(input *GrepInput, rootURI string) string {
	if input == nil {
		return ""
	}
	payload := grepSearchHashInput{
		Pattern:         strings.TrimSpace(input.Pattern),
		ExcludePattern:  strings.TrimSpace(input.ExcludePattern),
		Root:            strings.TrimSpace(rootURI),
		RootID:          strings.TrimSpace(input.RootID),
		Path:            strings.TrimSpace(input.Path),
		Recursive:       input.Recursive,
		Include:         append([]string(nil), input.Include...),
		Exclude:         append([]string(nil), input.Exclude...),
		CaseInsensitive: input.CaseInsensitive,
		Mode:            strings.TrimSpace(input.Mode),
		Bytes:           input.Bytes,
		Lines:           input.Lines,
		MaxFiles:        input.MaxFiles,
		MaxBlocks:       input.MaxBlocks,
		SkipBinary:      input.SkipBinary,
		MaxSize:         input.MaxSize,
		Concurrency:     input.Concurrency,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	sum := sha1.Sum(raw)
	return hex.EncodeToString(sum[:6])
}
