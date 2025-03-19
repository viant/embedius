package splitter

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"github.com/viant/embedius/document"
	"regexp"
)

// CodeSplitter implements content splitting optimized for code files
type CodeSplitter struct {
	maxFragmentSize int
	language        string
	functionRegex   *regexp.Regexp
	classRegex      *regexp.Regexp
}

// NewCodeSplitter creates a new code-aware splitter
func NewCodeSplitter(maxFragmentSize int, language string) *CodeSplitter {
	if maxFragmentSize <= 0 {
		maxFragmentSize = 4096
	}

	splitter := &CodeSplitter{
		maxFragmentSize: maxFragmentSize,
		language:        language,
	}

	// Set up language-specific patterns
	switch language {
	case "go":
		splitter.functionRegex = regexp.MustCompile(`func\s+(\w+)\s*\([^)]*\)`)
		// Go doesn't have classes but has structs and interfaces
		splitter.classRegex = regexp.MustCompile(`type\s+(\w+)\s+(struct|interface)`)
	case "java":
		splitter.functionRegex = regexp.MustCompile(`(public|private|protected)?\s+\w+\s+(\w+)\s*\([^)]*\)\s*\{`)
		splitter.classRegex = regexp.MustCompile(`(public|private|protected)?\s+class\s+(\w+)`)
	case "python":
		splitter.functionRegex = regexp.MustCompile(`def\s+(\w+)\s*\(`)
		splitter.classRegex = regexp.MustCompile(`class\s+(\w+)`)
	case "javascript", "typescript":
		splitter.functionRegex = regexp.MustCompile(`function\s+(\w+)\s*\(|(\w+)\s*=\s*function\s*\(|(\w+)\s*\([^)]*\)\s*{`)
		splitter.classRegex = regexp.MustCompile(`class\s+(\w+)`)
	default:
		// Generic patterns for other languages
		splitter.functionRegex = regexp.MustCompile(`function\s+(\w+)|def\s+(\w+)|func\s+(\w+)`)
		splitter.classRegex = regexp.MustCompile(`class\s+(\w+)|type\s+(\w+)`)
	}

	return splitter
}

// Split splits code files by trying to preserve logical structures
func (s *CodeSplitter) Split(data []byte, metadata map[string]interface{}) []*document.Fragment {
	fragments := make([]*document.Fragment, 0)
	if len(data) == 0 {
		return fragments
	}

	path, _ := metadata["path"].(string)

	// If the file is small enough, keep it as one fragment
	if len(data) <= s.maxFragmentSize {
		checksum := sha256.Sum256(data)
		checksumInt := int(binary.BigEndian.Uint32(checksum[:4]))
		fragments = append(fragments, &document.Fragment{
			Start:    0,
			End:      len(data),
			Checksum: checksumInt,
			Kind:     "code",
			Meta:     map[string]string{"path": path, "language": s.language},
		})
		return fragments
	}

	// Find logical boundaries (functions, classes)
	boundaries := s.findLogicalBoundaries(data)

	// Use logical boundaries to create fragments
	start := 0
	for _, boundary := range boundaries {
		if boundary-start > s.maxFragmentSize {
			// If the current segment is too large, split it further
			subFragments := s.splitBySize(path, data, start, boundary)
			fragments = append(fragments, subFragments...)
		} else {
			checksum := sha256.Sum256(data[start:boundary])
			checksumInt := int(binary.BigEndian.Uint32(checksum[:4]))
			fragments = append(fragments, &document.Fragment{
				Start:    start,
				End:      boundary,
				Checksum: checksumInt,
				Kind:     "code",
				Meta:     map[string]string{"path": path, "language": s.language},
			})
		}
		start = boundary
	}

	// Handle the remaining part
	if start < len(data) {
		subFragments := s.splitBySize(path, data, start, len(data))
		fragments = append(fragments, subFragments...)
	}

	return fragments
}

// findLogicalBoundaries identifies potential split points at logical code boundaries
func (s *CodeSplitter) findLogicalBoundaries(data []byte) []int {
	lines := bytes.Split(data, []byte{'\n'})
	boundaries := make([]int, 0)

	offset := 0
	for _, line := range lines {
		lineLen := len(line) + 1 // +1 for the newline character

		// Check if this line marks the beginning of a function or class
		if s.functionRegex.Match(line) || s.classRegex.Match(line) {
			if offset > 0 {
				boundaries = append(boundaries, offset)
			}
		}

		offset += lineLen
	}

	return boundaries
}

// splitBySize splits a range of data by size
func (s *CodeSplitter) splitBySize(path string, data []byte, start, end int) []*document.Fragment {
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
			Kind:     "code",
			Meta:     map[string]string{"path": path, "language": s.language},
		})

		i = fragmentEnd
	}

	return fragments
}
