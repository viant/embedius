package option

import (
	"bufio"
	"io"
	"strings"
)

// Options provides common configuration options between retriever and executor
type Options struct {

	// Exclusions contains patterns of files/directories to exclude
	Exclusions []string

	// InclusionPattern contains patterns of files/directories to include
	Inclusions []string

	// MaxFileSize is the maximum size of files to index in bytes
	MaxFileSize int
}

// Options returns a slice of Option functions based on the Options fields
func (o *Options) Options() []Option {
	var result []Option
	if o.MaxFileSize > 0 {
		result = append(result, WithMaxIndexableSize(o.MaxFileSize))
	}
	if o.Exclusions != nil {
		result = append(result, WithExclusionPatterns(o.Exclusions...))
	}
	if o.Inclusions != nil {
		result = append(result, WithInclusionPatterns(o.Inclusions...))
	}
	return result
}

// NewOptions creates a new Options instance with default values
func NewOptions(opts ...Option) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.Exclusions == nil {
		options.Exclusions = getDefaultPatterns()
	}
	return options
}

// Option is a function that modifies Options
type Option func(*Options)

// WithExclusionPatterns sets exclusion patterns
func WithExclusionPatterns(patterns ...string) Option {
	return func(o *Options) {
		o.Exclusions = append(o.Exclusions, patterns...)
	}
}

// WithMaxIndexableSize sets the maximum indexable file size
func WithMaxIndexableSize(size int) Option {
	return func(o *Options) {
		o.MaxFileSize = size
	}
}

// WithGitignore adds patterns from a .gitignore file
func WithGitignore(reader io.Reader) Option {
	return func(m *Options) {
		if patterns := parseGitignore(reader); len(patterns) > 0 {
			m.Exclusions = append(m.Exclusions, patterns...)
		}
	}
}

// WithInclusionPatterns adds patterns to include
func WithInclusionPatterns(patterns ...string) Option {
	return func(o *Options) {
		o.Inclusions = append(o.Inclusions, patterns...)
	}
}

// WithDefaultExclusionPatterns adds default exclusion patterns
func WithDefaultExclusionPatterns() Option {
	return func(m *Options) {
		m.Exclusions = append(m.Exclusions, getDefaultPatterns()...)
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
