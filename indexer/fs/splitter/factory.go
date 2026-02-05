package splitter

import (
	"path/filepath"
	"strings"
)

// Factory creates splitters based on file types and custom configurations
type Factory struct {
	defaultSplitter   Splitter
	extensionSplitter map[string]Splitter
	sizeSplitter      Splitter
}

// GetSplitter returns an appropriate splitter for the given file
func (f *Factory) GetSplitter(filePath string, fileSize int) Splitter {
	// Check for custom extension splitter
	ext := strings.ToLower(filepath.Ext(filePath))
	if splitter, ok := f.extensionSplitter[ext]; ok {
		return splitter
	}

	// Use size-based splitter for large files
	if fileSize > 1024*1024 { // 1 MB threshold
		return f.sizeSplitter
	}

	// Use default splitter
	return f.defaultSplitter
}

// NewFactory creates a splitter factory
func NewFactory(defaultMaxSize int) *Factory {
	if defaultMaxSize <= 0 {
		defaultMaxSize = 4096
	}

	factory := &Factory{
		defaultSplitter:   NewSizeSplitter(defaultMaxSize),
		extensionSplitter: make(map[string]Splitter),
		// Keep large-file splitter consistent with the configured default chunk size
		// (defaults to 4096 unless overridden by caller).
		sizeSplitter: NewSizeSplitter(defaultMaxSize),
	}

	// Register specialized splitters for common file types
	factory.RegisterExtensionSplitter(".go", NewCodeSplitter(defaultMaxSize, "go"))
	factory.RegisterExtensionSplitter(".java", NewCodeSplitter(defaultMaxSize, "java"))
	factory.RegisterExtensionSplitter(".py", NewCodeSplitter(defaultMaxSize, "python"))
	factory.RegisterExtensionSplitter(".js", NewCodeSplitter(defaultMaxSize, "javascript"))
	factory.RegisterExtensionSplitter(".ts", NewCodeSplitter(defaultMaxSize, "typescript"))
	factory.RegisterExtensionSplitter(".md", NewMarkdownSplitter(defaultMaxSize))
	factory.RegisterExtensionSplitter(".pdf", NewPDFSplitter(defaultMaxSize))
	factory.RegisterExtensionSplitter(".docx", NewDOCXSplitter(defaultMaxSize))
	return factory
}

// RegisterExtensionSplitter registers a custom splitter for a file extension
func (f *Factory) RegisterExtensionSplitter(ext string, splitter Splitter) {
	f.extensionSplitter[strings.ToLower(ext)] = splitter
}
