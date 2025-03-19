package document

import (
	"time"
)

// Entry represents a stored document with its metadata
type Entry struct {
	ID        string    // Unique identifier for the entry
	ModTime   time.Time // Last modification time
	Hash      uint64    // Hash of the content for change detection
	Fragments Fragments // Document fragments
}
