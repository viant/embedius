package splitter

import (
	"crypto/sha256"
	"encoding/binary"
	"github.com/viant/embedius/document"
)

// SizeSplitter implements content splitting based purely on size
type SizeSplitter struct {
	maxFragmentSize int
}

// NewSizeSplitter creates a new SizeSplitter
func NewSizeSplitter(maxFragmentSize int) *SizeSplitter {
	if maxFragmentSize <= 0 {
		maxFragmentSize = 1024 // Default for large files
	}
	return &SizeSplitter{
		maxFragmentSize: maxFragmentSize,
	}
}

// Split splits content based purely on size without context awareness
func (s *SizeSplitter) Split(data []byte, metadata map[string]interface{}) []*document.Fragment {
	fragments := make([]*document.Fragment, 0)
	dataLen := len(data)

	if dataLen == 0 {
		return fragments
	}

	path, _ := metadata["path"].(string)

	for start := 0; start < dataLen; {
		end := start + s.maxFragmentSize
		if end > dataLen {
			end = dataLen
		}

		checksum := sha256.Sum256(data[start:end])
		checksumInt := int(binary.BigEndian.Uint32(checksum[:4]))

		fragment := &document.Fragment{
			Start:    start,
			End:      end,
			Checksum: checksumInt,
			Kind:     "binary",
			Meta:     map[string]string{"path": path},
		}

		fragments = append(fragments, fragment)
		start = end
	}

	return fragments
}
