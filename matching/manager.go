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

	for _, pattern := range m.options.Exclusions {
		pattern = strings.TrimSpace(pattern)
		// Skip comments or empty lines
		if pattern == "" || strings.HasPrefix(pattern, "#") {
			continue
		}

		if m.isExcluded(path, pattern) {
			return true
		}
	}

	return false
}

func (m *Manager) isExcluded(path string, pattern string) bool {
	// Direct substring match for directory paths (e.g., node_modules/)
	if strings.HasSuffix(pattern, "/") && strings.Contains(path, pattern) {
		return true
	}

	// Handle relative patterns with leading **/ (match anywhere in path)
	if strings.HasPrefix(pattern, "**/") {
		suffix := strings.TrimPrefix(pattern, "**/")
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

func (m *Manager) isIncluded(path string) bool {
	for _, pattern := range m.options.Inclusions {
		pattern = strings.TrimSpace(pattern)
		// Skip comments or empty lines
		if pattern == "" || strings.HasPrefix(pattern, "#") {
			continue
		}

		// Direct substring match for directory paths
		if strings.HasSuffix(pattern, "/") && strings.Contains(path, pattern) {
			return true
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
	}
	return false
}
