package matching

import (
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/matching/option"
	"path/filepath"
	"strings"
)

// Manager handles file/directory exclusion rules for retrieval and execution
type Manager struct {
	options *option.Options
	fs      afs.Service
}

// Option defines a functional option for configuring the Manager
type Option func(*Manager)

// New creates a new exclusion manager with the given options
func New(opts ...option.Option) *Manager {
	options := option.NewOptions(opts...)
	manager := &Manager{
		options: options,
		fs:      afs.New(),
	}
	return manager
}

// IsExcluded checks if a path should be excluded based on the patterns
func (m *Manager) IsExcluded(location string, size int) bool {
	if m.options.MaxFileSize > 0 {
		if size > m.options.MaxFileSize {
			return true
		}
	}

	//storage object implements os.FileInfo
	path := url.Path(location)
	// Normalize path to use forward slashes
	path = filepath.ToSlash(path)

	if len(m.options.Inclusions) > 0 {
		included := m.isIncluded(path)
		if !included {
			return true
		}
	}

	excluded := false
	for _, pattern := range m.options.Exclusions {
		pattern = strings.TrimSpace(pattern)
		// Skip comments or empty lines
		if pattern == "" || strings.HasPrefix(pattern, "#") {
			continue
		}

		negated := strings.HasPrefix(pattern, "!")
		if negated {
			pattern = strings.TrimSpace(strings.TrimPrefix(pattern, "!"))
			if pattern == "" {
				continue
			}
		}

		matched := false
		if strings.HasPrefix(pattern, "git:") {
			pattern = strings.TrimPrefix(pattern, "git:")
			if pattern != "" {
				matched = gitignoreMatch(path, pattern)
			}
		} else {
			matched = m.isExcluded(path, pattern)
		}

		if matched {
			if negated {
				excluded = false
			} else {
				excluded = true
			}
		}
	}

	return excluded
}

func (m *Manager) isExcluded(path string, pattern string) bool {
	// Direct substring match for directory paths (e.g., node_modules/)
	if strings.HasSuffix(pattern, "/") && matchDirPattern(path, pattern) {
		return true
	}

	// Handle directory patterns like **/dir/** or dir/** (match anywhere in path)
	if strings.HasSuffix(pattern, "/**") {
		dir := strings.TrimSuffix(pattern, "/**")
		dir = strings.TrimPrefix(dir, "**/")
		if dir != "" {
			if path == dir || strings.HasPrefix(path, dir+"/") || strings.Contains(path, "/"+dir+"/") {
				return true
			}
		}
	}

	// Handle relative patterns with leading **/ (match anywhere in path)
	if strings.HasPrefix(pattern, "**/") {
		suffix := strings.TrimPrefix(pattern, "**/")
		if matched, _ := filepath.Match(suffix, path); matched {
			return true
		}
		base := filepath.Base(path)
		if matched, _ := filepath.Match(suffix, base); matched {
			return true
		}
		return strings.HasSuffix(path, suffix) || strings.Contains(path, "/"+suffix)
	}

	// Try filepath pattern matching for wildcard patterns
	matched, err := filepath.Match(pattern, path)
	if err == nil && matched {
		return true
	}

	// Match against the basename
	base := filepath.Base(path)
	matched, err = filepath.Match(pattern, base)
	if err == nil && matched {
		return true
	}

	// Try to match pattern against any part of the path
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if matched, _ := filepath.Match(pattern, part); matched {
			return true
		}
	}

	// For patterns like *.go, try matching against the file extension
	if strings.HasPrefix(pattern, "*.") {
		extension := "." + strings.Split(pattern, ".")[1]
		if strings.HasSuffix(path, extension) {
			return true
		}
	}

	return false
}

func gitignoreMatch(path string, pattern string) bool {
	anchored := strings.HasPrefix(pattern, "/")
	if !anchored {
		trimmed := strings.TrimSuffix(pattern, "/")
		anchored = strings.Contains(trimmed, "/")
	}
	if anchored {
		pattern = strings.TrimPrefix(pattern, "/")
	}
	dirOnly := strings.HasSuffix(pattern, "/")
	if dirOnly {
		pattern = strings.TrimSuffix(pattern, "/")
	}
	if pattern == "" {
		return false
	}

	path = strings.TrimPrefix(path, "/")
	if dirOnly && !strings.Contains(path, "/") {
		return false
	}
	patSegments := splitPatternSegments(pattern)
	if dirOnly && (len(patSegments) == 0 || patSegments[len(patSegments)-1] != "**") {
		patSegments = append(patSegments, "**")
	}
	pathSegments := splitPathSegments(path)

	if anchored {
		if len(patSegments) == 1 && patSegments[0] != "**" {
			if len(pathSegments) == 0 {
				return false
			}
			return segmentMatch(patSegments[0], pathSegments[0])
		}
		return matchSegments(pathSegments, patSegments)
	}
	if len(patSegments) == 1 && patSegments[0] != "**" {
		for _, seg := range pathSegments {
			if segmentMatch(patSegments[0], seg) {
				return true
			}
		}
		return false
	}
	for i := 0; i < len(pathSegments); i++ {
		if matchSegments(pathSegments[i:], patSegments) {
			return true
		}
	}
	return false
}

func splitPathSegments(path string) []string {
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func splitPatternSegments(pattern string) []string {
	if pattern == "" {
		return nil
	}
	parts := strings.Split(pattern, "/")
	out := parts[:0]
	for _, part := range parts {
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func matchSegments(pathSegs []string, patSegs []string) bool {
	if len(patSegs) == 0 {
		return len(pathSegs) == 0
	}
	if len(pathSegs) == 0 {
		for _, p := range patSegs {
			if p != "**" {
				return false
			}
		}
		return true
	}
	if patSegs[0] == "**" {
		for i := 0; i <= len(pathSegs); i++ {
			if matchSegments(pathSegs[i:], patSegs[1:]) {
				return true
			}
		}
		return false
	}
	if !segmentMatch(patSegs[0], pathSegs[0]) {
		return false
	}
	return matchSegments(pathSegs[1:], patSegs[1:])
}

func segmentMatch(pattern string, segment string) bool {
	if pattern == "**" {
		return true
	}
	matched, err := filepath.Match(pattern, segment)
	if err != nil {
		return false
	}
	return matched
}

func matchDirPattern(path string, pattern string) bool {
	dir := strings.TrimSuffix(pattern, "/")
	if dir == "" {
		return false
	}
	return path == dir || strings.HasPrefix(path, dir+"/") || strings.Contains(path, "/"+dir+"/")
}

func (m *Manager) isIncluded(path string) bool {
	included := false
	for _, pattern := range m.options.Inclusions {
		pattern = strings.TrimSpace(pattern)
		// Skip comments or empty lines
		if pattern == "" || strings.HasPrefix(pattern, "#") {
			continue
		}
		negated := strings.HasPrefix(pattern, "!")
		if negated {
			pattern = strings.TrimSpace(strings.TrimPrefix(pattern, "!"))
			if pattern == "" {
				continue
			}
		}

		matched := false
		if strings.HasPrefix(pattern, "git:") {
			pattern = strings.TrimPrefix(pattern, "git:")
			if pattern != "" {
				matched = gitignoreMatch(path, pattern)
			}
		} else {
			matched = matchIncludedPattern(path, pattern)
		}
		if matched {
			if negated {
				included = false
			} else {
				included = true
			}
		}
	}
	return included
}

func matchIncludedPattern(path string, pattern string) bool {
	// Direct substring match for directory paths
	if strings.HasSuffix(pattern, "/") && matchDirPattern(path, pattern) {
		return true
	}

	// Handle relative patterns with leading **/ (match anywhere in path)
	if strings.HasPrefix(pattern, "**/") {
		suffix := strings.TrimPrefix(pattern, "**/")
		if matched, _ := filepath.Match(suffix, path); matched {
			return true
		}
		base := filepath.Base(path)
		if matched, _ := filepath.Match(suffix, base); matched {
			return true
		}
		if strings.HasSuffix(path, suffix) || strings.Contains(path, "/"+suffix) {
			return true
		}
	}

	// Try filepath pattern matching for wildcard patterns
	matched, err := filepath.Match(pattern, path)
	if err == nil && matched {
		return true
	}

	// Match against the basename
	base := filepath.Base(path)
	matched, err = filepath.Match(pattern, base)
	if err == nil && matched {
		return true
	}

	// For patterns like *.go, try matching against the file extension
	if strings.HasPrefix(pattern, "*.") {
		extension := "." + strings.Split(pattern, ".")[1]
		if strings.HasSuffix(path, extension) {
			return true
		}
	}
	return false
}
