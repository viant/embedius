package matching

import (
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/embedius/indexer/fs/option"
	"path/filepath"
	"strings"
)

// Manager handles file/directory exclusion rules for retrieval and execution
type Manager struct {
	options *option.Options
}

// Option defines a functional option for configuring the Manager
type Option func(*Manager)

// New creates a new exclusion manager with the given options
func New(opts ...option.Option) *Manager {
	options := option.NewOptions(opts...)
	manager := &Manager{
		options: options,
	}
	return manager
}

// IsExcluded checks if a path should be excluded based on the patterns
func (m *Manager) IsExcluded(object storage.Object) bool {
	if m.options.MaxInclusionFileSize > 0 {
		if object.Size() > int64(m.options.MaxInclusionFileSize) {
			return true
		}
	}

	//storage object implements os.FileInfo
	path := url.Path(object.URL())
	// Normalize path to use forward slashes
	path = filepath.ToSlash(path)

	if len(m.options.InclusionPatterns) > 0 {
		included := m.isIncluded(path)
		if !included {
			return true
		}
	}

	for _, pattern := range m.options.ExclusionPatterns {
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
	// Direct substring match (common case for directories like node_modules)
	if strings.Contains(path, pattern) {
		return true
	}

	// Try filepath pattern matching (like .gitignore patterns)
	// Handle leading/trailing slashes and wildcards
	cleanPattern := strings.TrimPrefix(pattern, "/")
	if matched, _ := filepath.Match(cleanPattern, path); matched {
		return true
	}
	if matched, _ := filepath.Match("*/"+cleanPattern, path); matched {
		return true
	}

	// Match just basename
	baseName := filepath.Base(path)
	if pattern == baseName || strings.HasSuffix(pattern, "/"+baseName) {
		return true
	}
	return false
}

func (m *Manager) isIncluded(path string) bool {
	var included bool
	for _, pattern := range m.options.InclusionPatterns {
		pattern = strings.TrimSpace(pattern)
		// Skip comments or empty lines
		if pattern == "" || strings.HasPrefix(pattern, "#") {
			continue
		}
		if strings.Contains(path, pattern) {
			included = true
			break
		}
	}
	return included
}
