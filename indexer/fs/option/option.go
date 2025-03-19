package option

import (
	"bufio"
	"io"
	"strings"
)

// Options provides common configuration options between retriever and executor
type Options struct {

	// ExclusionPatterns contains patterns of files/directories to exclude
	ExclusionPatterns []string

	// InclusionPattern contains patterns of files/directories to include
	InclusionPatterns []string

	// MaxInclusionFileSize is the maximum size of files to index in bytes
	MaxInclusionFileSize int

	// MaxResponseSize limits the size of files sent to LLM
	MaxResponseSize int
}

func (o *Options) Options() []Option {
	var result []Option
	if o.MaxInclusionFileSize > 0 {
		result = append(result, WithMaxIndexableSize(o.MaxInclusionFileSize))
	}
	if o.MaxResponseSize > 0 {
		result = append(result, WithMaxResponseSize(o.MaxResponseSize))
	}
	if o.ExclusionPatterns != nil {
		result = append(result, WithExclusionPatterns(o.ExclusionPatterns...))
	}
	if o.InclusionPatterns != nil {
		result = append(result, WithInclusionPatterns(o.InclusionPatterns...))
	}
	return result
}

// NewOptions creates a new Options instance with default values
func NewOptions(opts ...Option) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.ExclusionPatterns == nil {
		options.ExclusionPatterns = getDefaultPatterns()
	}
	return options
}

// Option is a function that modifies Options
type Option func(*Options)

// WithExclusionPatterns sets exclusion patterns
func WithExclusionPatterns(patterns ...string) Option {
	return func(o *Options) {
		o.ExclusionPatterns = append(o.ExclusionPatterns, patterns...)
	}
}

// WithMaxIndexableSize sets the maximum indexable file size
func WithMaxIndexableSize(size int) Option {
	return func(o *Options) {
		o.MaxInclusionFileSize = size
	}
}

// WithMaxResponseSize sets the maximum size for LLM responses
func WithMaxResponseSize(size int) Option {
	return func(o *Options) {
		o.MaxResponseSize = size
	}
}

// WithGitignore adds patterns from a .gitignore file
func WithGitignore(reader io.Reader) Option {
	return func(m *Options) {
		if patterns := parseGitignore(reader); len(patterns) > 0 {
			m.ExclusionPatterns = append(m.ExclusionPatterns, patterns...)
		}
	}
}

// WithInclusionPatterns adds patterns to include
func WithInclusionPatterns(patterns ...string) Option {
	return func(o *Options) {
		o.InclusionPatterns = append(o.InclusionPatterns, patterns...)
	}
}

// WithDefaultExclusionPatterns adds default exclusion patterns
func WithDefaultExclusionPatterns() Option {
	return func(m *Options) {
		m.ExclusionPatterns = append(m.ExclusionPatterns, getDefaultPatterns()...)
	}
}

// getDefaultPatterns returns commonly excluded paths and file patterns
func getDefaultPatterns() []string {
	return []string{
		// Directories
		"node_modules/",
		".git/",
		".github/",
		".vscode/",
		".idea/",
		"dist/",
		"build/",
		"target/",
		"bin/",
		"obj/",
		".next/",
		"__pycache__/",
		".pytest_cache/",
		"vendor/",
		"coverage/",

		// Files
		".DS_Store",
		"*.min.js",
		"*.min.css",
		"*.map",
		"*.wasm",
		"*.pb.go",
		"*.lock",
		"package-lock.json",
		"yarn.lock",
		"*.log",
		"*.swp",
		".env",
		"*.bak",
		"*.tmp",
		"*.dll",
		"*.exe",
	}
}

// parseGitignore reads .gitignore-style patterns from a reader
func parseGitignore(reader io.Reader) []string {
	var patterns []string
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, line)
	}

	return patterns
}
