package splitter

import (
	"github.com/viant/embedius/document"
)

// Splitter defines the interface for content splitting strategies
type Splitter interface {
	// Split divides content into logical fragments
	Split(data []byte, metadata map[string]interface{}) []*document.Fragment
}

// ContentSplitter can return transformed content for indexing.
type ContentSplitter interface {
	SplitWithContent(data []byte, metadata map[string]interface{}) ([]*document.Fragment, []byte)
}
