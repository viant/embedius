package splitter

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"github.com/viant/embedius/document"
)

// MarkdownSplitter handles Markdown content splitting based on headers
type MarkdownSplitter struct {
	maxFragmentSize int
}

// NewMarkdownSplitter creates a new Markdown splitter
func NewMarkdownSplitter(maxFragmentSize int) *MarkdownSplitter {
	if maxFragmentSize <= 0 {
		maxFragmentSize = 4096
	}
	return &MarkdownSplitter{
		maxFragmentSize: maxFragmentSize,
	}
}

// Split splits markdown content by headings when possible
func (s *MarkdownSplitter) Split(data []byte, metadata map[string]interface{}) []*document.Fragment {
	fragments := make([]*document.Fragment, 0)
	if len(data) == 0 {
		return fragments
	}

	path, _ := metadata["path"].(string)

	// If the markdown file is small, keep it as one fragment
	if len(data) <= s.maxFragmentSize {
		checksum := sha256.Sum256(data)
		checksumInt := int(binary.BigEndian.Uint32(checksum[:4]))
		fragments = append(fragments, &document.Fragment{
			Start:    0,
			End:      len(data),
			Checksum: checksumInt,
			Kind:     "markdown",
			Meta:     map[string]string{"path": path},
		})
		return fragments
	}

	// Find heading markers as logical split points
	boundaries := s.findHeadingBoundaries(data)

	// Use logical boundaries to create fragments
	start := 0
	for _, boundary := range boundaries {
		if boundary-start > s.maxFragmentSize {
			// If the current segment is too large, split it further
			chunks := s.splitBySize(data, start, boundary, path)
			fragments = append(fragments, chunks...)
		} else {
			checksum := sha256.Sum256(data[start:boundary])
			checksumInt := int(binary.BigEndian.Uint32(checksum[:4]))
			fragments = append(fragments, &document.Fragment{
				Start:    start,
				End:      boundary,
				Checksum: checksumInt,
				Kind:     "markdown",
				Meta:     map[string]string{"path": path},
			})
		}
		start = boundary
	}

	// Handle the remaining part
	if start < len(data) {
		chunks := s.splitBySize(data, start, len(data), path)
		fragments = append(fragments, chunks...)
	}

	return fragments
}

// findHeadingBoundaries finds markdown heading boundaries
func (s *MarkdownSplitter) findHeadingBoundaries(data []byte) []int {
	boundaries := make([]int, 0)
	lines := bytes.Split(data, []byte{'\n'})

	offset := 0
	for _, line := range lines {
		lineLen := len(line) + 1 // +1 for the newline character

		// Check for Markdown headings (both # style and underline style)
		if len(line) > 0 && line[0] == '#' {
			if offset > 0 {
				boundaries = append(boundaries, offset)
			}
		} else if len(line) > 2 && (bytes.HasPrefix(bytes.TrimSpace(line), []byte("--")) ||
			bytes.HasPrefix(bytes.TrimSpace(line), []byte("=="))) {
			if offset > 0 {
				boundaries = append(boundaries, offset-len(lines[len(lines)-1])-1)
			}
		}

		offset += lineLen
	}

	return boundaries
}

// splitBySize splits a range of data by size
func (s *MarkdownSplitter) splitBySize(data []byte, start, end int, path string) []*document.Fragment {
	fragments := make([]*document.Fragment, 0)

	for i := start; i < end; {
		fragmentEnd := i + s.maxFragmentSize
		if fragmentEnd > end {
			fragmentEnd = end
		}

		// Try to find a good split point (newline)
		if fragmentEnd < end {
			for j := fragmentEnd; j > i+s.maxFragmentSize/2; j-- {
				if data[j] == '\n' {
					fragmentEnd = j + 1
					break
				}
			}
		}

		checksum := sha256.Sum256(data[i:fragmentEnd])
		checksumInt := int(binary.BigEndian.Uint32(checksum[:4]))

		fragments = append(fragments, &document.Fragment{
			Start:    i,
			End:      fragmentEnd,
			Checksum: checksumInt,
			Kind:     "markdown",
			Meta:     map[string]string{"path": path},
		})

		i = fragmentEnd
	}

	return fragments
}
