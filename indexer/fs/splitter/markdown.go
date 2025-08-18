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
	lineLen := 0
	prevLine := []byte{}
	prevLineLen := 0

	for i, line := range lines {
		lineLen = len(line) + 1 // +1 for the newline character

		// Check for Markdown headings (# ATX style and underline ---/=== Setext style)
		//
		// ATX-style headers (# style) requires a space after the # characters
		//
		// Setext-style - CommonMark, GitHub, Obsidian, and most modern Markdown engines require
		// rules of valid H1/H2 heading:
		// Must use at least 3 `=` or at least 3 `-`  characters
		// No blank line above
		// No extra characters allowed on the underline
		// Max 3 leading spaces before `===` or `---`

		if len(line) > 0 && isATXHeader(line) { // Check for ATX style headings (e.g., # Heading, ## Subheading)
			if offset > 0 {
				boundaries = append(boundaries, offset)
			}
		} else if i > 1 { // Check for Setext style headings (e.g., Heading\n=== or Heading\n---)
			ok := isSetextUnderline(line) // Check if the line is a valid Setext underline
			if ok && !isBlank(prevLine) {
				// found setext-style header
				boundaries = append(boundaries, offset-prevLineLen)
			}
		}

		prevLine = line
		prevLineLen = lineLen
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

// Checks if the line is a valid Setext underline (=== or ---)
func isSetextUnderline(line []byte) bool {
	if len(line) < 3 {
		return false
	}

	if line[0] != ' ' && line[0] != '-' && line[0] != '=' {
		return false
	}

	leadingSpaces := len(line) - len(bytes.TrimLeft(line, " "))
	if leadingSpaces > 3 {
		return false
	}

	trimmed := bytes.TrimSpace(line)
	if len(trimmed) < 3 {
		return false
	}

	first := trimmed[0]
	if first != '=' && first != '-' {
		return false
	}

	for _, ch := range trimmed {
		if ch != first {
			return false
		}
	}

	return true
}

// Checks if the line is blank (contains only whitespace)
func isBlank(line []byte) bool {
	return len(bytes.TrimSpace(line)) == 0
}

// Checks if the line is ATX-style header (starts with # and has a space or nothing after)
func isATXHeader(line []byte) bool {
	if len(line) == 0 {
		return false
	}

	if line[0] != ' ' && line[0] != '#' {
		return false
	}

	trimmed := bytes.TrimLeft(line, " ")
	leadingSpaces := len(line) - len(trimmed)
	if leadingSpaces > 3 {
		return false
	}

	if len(trimmed) == 0 {
		return false
	}

	first := trimmed[0]
	if first != '#' {
		return false
	}

	for i, ch := range trimmed {
		if ch == first && i > 5 {
			return false // More than 6 '#' characters is not valid
		}

		if ch != first {
			if ch == ' ' { // Allow up to 6 '#' characters followed by a space
				return true
			} else {
				return false
			}
		}

	}

	// Allow up to 6 '#' characters followed by nothing
	return true
}
