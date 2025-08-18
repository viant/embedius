package splitter

import (
	"github.com/viant/embedius/document"
	"testing"
)

func TestMarkdownSplitter_Split(t *testing.T) {
	testCases := []struct {
		name               string
		maxFragmentSize    int
		data               []byte
		metadata           map[string]interface{}
		expectedCount      int
		checkFirstFragment func(t *testing.T, fragment *document.Fragment)
		checkAllFragments  func(t *testing.T, fragments []*document.Fragment)
	}{
		{
			name:               "Empty data",
			maxFragmentSize:    4096,
			data:               []byte{},
			metadata:           map[string]interface{}{"path": "test.md"},
			expectedCount:      0,
			checkFirstFragment: nil,
			checkAllFragments:  nil,
		},
		{
			name:            "Small content",
			maxFragmentSize: 4096,
			data:            []byte("# Small markdown\nThis is a small markdown document."),
			metadata:        map[string]interface{}{"path": "test.md"},
			expectedCount:   1,
			checkFirstFragment: func(t *testing.T, fragment *document.Fragment) {
				if fragment.Start != 0 {
					t.Errorf("Expected Start to be 0, got %d", fragment.Start)
				}
				if fragment.End != 51 {
					t.Errorf("Expected End to be 51, got %d", fragment.End)
				}
				if fragment.Kind != "markdown" {
					t.Errorf("Expected Kind to be 'markdown', got %s", fragment.Kind)
				}
				if path, ok := fragment.Meta["path"]; !ok || path != "test.md" {
					t.Errorf("Expected Meta[path] to be 'test.md', got %s", path)
				}
			},
			checkAllFragments: nil,
		},
		{
			name:            "Content with headings",
			maxFragmentSize: 4096,
			data:            []byte("# First heading\nContent under first heading.\n\n# Second heading\nContent under second heading."),
			metadata:        map[string]interface{}{"path": "test.md"},
			expectedCount:   1,
			checkFirstFragment: func(t *testing.T, fragment *document.Fragment) {
				if fragment.Start != 0 {
					t.Errorf("Expected Start to be 0, got %d", fragment.Start)
				}
				if fragment.End != 92 {
					t.Errorf("Expected End to be 92, got %d", fragment.End)
				}
			},
			checkAllFragments: nil,
		},
		{
			name:               "Large content",
			maxFragmentSize:    20,
			data:               []byte("This is a larger markdown document that will need to be split by size rather than by headings."),
			metadata:           map[string]interface{}{"path": "test.md"},
			expectedCount:      5, // The exact number depends on how the splitting logic works
			checkFirstFragment: nil,
			checkAllFragments:  nil,
		},
		{
			name:            "Content with underline style headings",
			maxFragmentSize: 4096,
			data:            []byte("First heading\n============\nContent under first heading.\n\nSecond heading\n-------------\nContent under second heading."),
			metadata:        map[string]interface{}{"path": "test.md"},
			expectedCount:   1,
			checkFirstFragment: func(t *testing.T, fragment *document.Fragment) {
				if fragment.Start != 0 {
					t.Errorf("Expected Start to be 0, got %d", fragment.Start)
				}
			},
			checkAllFragments: nil,
		},
		{
			name:               "Content with headings that need to be split by size",
			maxFragmentSize:    20,
			data:               []byte("# First heading\nThis is a long content under the first heading that exceeds the max fragment size.\n\n# Second heading\nThis is another long content under the second heading."),
			metadata:           map[string]interface{}{"path": "test.md"},
			expectedCount:      10, // The exact number depends on how the splitting logic works
			checkFirstFragment: nil,
			checkAllFragments: func(t *testing.T, fragments []*document.Fragment) {
				// Check that we have at least one fragment for each heading section
				if len(fragments) < 2 {
					t.Errorf("Expected at least 2 fragments, got %d", len(fragments))
				}
			},
		},
		{
			name:            "Content with no path in metadata",
			maxFragmentSize: 4096,
			data:            []byte("# Heading\nContent."),
			metadata:        map[string]interface{}{},
			expectedCount:   1,
			checkFirstFragment: func(t *testing.T, fragment *document.Fragment) {
				if fragment.Meta["path"] != "" {
					t.Errorf("Expected empty path, got %s", fragment.Meta["path"])
				}
			},
			checkAllFragments: nil,
		},
		{
			name:            "Content with nil metadata",
			maxFragmentSize: 4096,
			data:            []byte("# Heading\nContent."),
			metadata:        nil,
			expectedCount:   1,
			checkFirstFragment: func(t *testing.T, fragment *document.Fragment) {
				if fragment.Meta["path"] != "" {
					t.Errorf("Expected empty path, got %s", fragment.Meta["path"])
				}
			},
			checkAllFragments: nil,
		},
		{
			name:               "Content with very small maxFragmentSize",
			maxFragmentSize:    1,
			data:               []byte("ABC"),
			metadata:           map[string]interface{}{"path": "test.md"},
			expectedCount:      3,
			checkFirstFragment: nil,
			checkAllFragments: func(t *testing.T, fragments []*document.Fragment) {
				if len(fragments) != 3 {
					t.Errorf("Expected 3 fragments, got %d", len(fragments))
				} else {
					if fragments[0].End-fragments[0].Start != 1 {
						t.Errorf("Expected fragment size to be 1, got %d", fragments[0].End-fragments[0].Start)
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			splitter := NewMarkdownSplitter(tc.maxFragmentSize)
			fragments := splitter.Split(tc.data, tc.metadata)

			if tc.name == "Content from test_data.md file" {
				// For the test_data.md file, we just check that at least one fragment is created
				if len(fragments) == 0 {
					t.Errorf("Expected at least one fragment, got 0")
				}
			} else if len(fragments) != tc.expectedCount {
				t.Errorf("Expected %d fragments, got %d", tc.expectedCount, len(fragments))
			}

			// Run custom first fragment checks if provided
			if len(fragments) > 0 && tc.checkFirstFragment != nil {
				tc.checkFirstFragment(t, fragments[0])
			}

			// Run custom all fragments checks if provided
			if tc.checkAllFragments != nil {
				tc.checkAllFragments(t, fragments)
			}

			// Check that fragments don't overlap and cover the entire document
			if len(fragments) > 0 {
				for i := 0; i < len(fragments)-1; i++ {
					if fragments[i].End != fragments[i+1].Start {
						t.Errorf("Fragment %d ends at %d but fragment %d starts at %d",
							i, fragments[i].End, i+1, fragments[i+1].Start)
					}
				}

				if fragments[0].Start != 0 {
					t.Errorf("First fragment should start at 0, got %d", fragments[0].Start)
				}

				if fragments[len(fragments)-1].End != len(tc.data) {
					t.Errorf("Last fragment should end at %d, got %d",
						len(tc.data), fragments[len(fragments)-1].End)
				}
			}
		})
	}
}

func TestMarkdownSplitter_findHeadingBoundaries(t *testing.T) {
	testCases := []struct {
		name               string
		data               []byte
		expectedBoundaries []int
	}{
		{
			name:               "Empty data",
			data:               []byte{},
			expectedBoundaries: []int{},
		},
		{
			name:               "No headings",
			data:               []byte("This is a paragraph.\nThis is another paragraph."),
			expectedBoundaries: []int{},
		},
		{
			name:               "Hash style headings",
			data:               []byte("# First heading\nContent.\n\n# Second heading\nMore content."),
			expectedBoundaries: []int{26},
		},
		{
			name:               "Underline style headings",
			data:               []byte("Valid\n===\nMore content.\nFirst heading\n=============\nContent.\n\nSecond heading\n-------------\nMore content."),
			expectedBoundaries: []int{24, 62},
		},
		{
			name:               "Mixed style headings",
			data:               []byte("Valid\n===\nMore content.\n# First heading\nContent.\n\nSecond heading\n-------------\nMore content."),
			expectedBoundaries: []int{24, 50},
		},
		{
			name:               "Multiple ATX headings of different levels",
			data:               []byte("# H1\nContent.\n\n## H2\nMore content.\n\n### H3\nEven more content."),
			expectedBoundaries: []int{15, 36},
		},
		{
			name:               "ATX headings with leading spaces",
			data:               []byte("Valid\n===\nMore content.\n   # H1 with spaces\nContent.\n\n  ## H2 with spaces\nMore content."),
			expectedBoundaries: []int{24, 54},
		},
		{
			name:               "Setext headings with different underline lengths",
			data:               []byte("Valid\n===\nMore content.\nH1\n===\nContent.\n\nH2\n-----\nMore content."),
			expectedBoundaries: []int{24, 41},
		},
		{
			name:               "Setext headings with leading spaces in underline",
			data:               []byte("Valid\n===\nMore content.\nH1\n   ===\nContent.\n\nH2\n  ---\nMore content."),
			expectedBoundaries: []int{24, 44},
		},
		{
			name:               "Invalid ATX heading (no space after #)",
			data:               []byte("Valid\n===\nMore content.\n#Invalid\nContent.\n\n# Valid\nMore content."),
			expectedBoundaries: []int{43},
		},
		{
			name:               "Invalid ATX heading (too many #)",
			data:               []byte("Valid\n===\nMore content.\n####### Invalid\nContent.\n\n# Valid\nMore content."),
			expectedBoundaries: []int{50},
		},
		{
			name:               "Invalid Setext heading (too short underline)",
			data:               []byte("Valid\n===\nMore content.\nInvalid\n==\nContent.\n\nValid\n===\nMore content."),
			expectedBoundaries: []int{45},
		},
		{
			name:               "Invalid Setext heading (mixed characters in underline)",
			data:               []byte("Valid\n===\nMore content.\nInvalid\n===-\nContent.\n\nValid\n===\nMore content."),
			expectedBoundaries: []int{47},
		},
		{
			name:               "Blank line before Setext heading",
			data:               []byte("Valid\n===\nContent.\n\n\nInvalid\n\n===\nMore content.\nValid\n===\nContent."),
			expectedBoundaries: []int{48},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			splitter := NewMarkdownSplitter(4096)
			boundaries := splitter.findHeadingBoundaries(tc.data)

			if len(boundaries) != len(tc.expectedBoundaries) {
				t.Errorf("Expected %d boundaries, got %d", len(tc.expectedBoundaries), len(boundaries))
			}

			for i, expected := range tc.expectedBoundaries {
				if i < len(boundaries) && boundaries[i] != expected {
					t.Errorf("Expected boundary at position %d to be %d, got %d", i, expected, boundaries[i])
				}
			}
		})
	}
}

func TestMarkdownSplitter_splitBySize(t *testing.T) {
	testCases := []struct {
		name            string
		maxFragmentSize int
		data            []byte
		start           int
		end             int
		expectedCount   int
		checkFragments  func(t *testing.T, fragments []*document.Fragment)
	}{

		{
			name:            "Small content",
			maxFragmentSize: 100,
			data:            []byte("This is a small content."),
			start:           0,
			end:             24,
			expectedCount:   1,
			checkFragments:  nil,
		},
		{
			name:            "Content larger than max size",
			maxFragmentSize: 10,
			data:            []byte("This is a longer content that needs to be split into multiple fragments."),
			start:           0,
			end:             72,
			expectedCount:   8, // The exact number depends on how the splitting logic works
			checkFragments:  nil,
		},
		{
			name:            "Content with newlines",
			maxFragmentSize: 20,
			data:            []byte("Line 1\nLine 2\nLine 3\nLine 4\nLine 5"),
			start:           0,
			end:             34,
			expectedCount:   2, // Should split at newlines when possible
			checkFragments: func(t *testing.T, fragments []*document.Fragment) {
				// Check that the split happened at a newline
				if fragments[0].End != 21 { // After "Line 1\nLine 2\nLine 3\n"
					t.Errorf("Expected split at newline (position 21), got %d", fragments[0].End)
				}
			},
		},
		{
			name:            "No good split point",
			maxFragmentSize: 10,
			data:            []byte("ThisIsALongWordWithoutSpacesOrNewlines"),
			start:           0,
			end:             38,
			expectedCount:   4, // Will have to split without a good split point
			checkFragments: func(t *testing.T, fragments []*document.Fragment) {
				// Check that fragments are approximately maxFragmentSize in length
				for i, fragment := range fragments {
					if i < len(fragments)-1 { // All but the last fragment
						fragmentSize := fragment.End - fragment.Start
						if fragmentSize != 10 {
							t.Errorf("Fragment %d size expected to be 10, got %d", i, fragmentSize)
						}
					}
				}
			},
		},
		{
			name:            "Split about half of maxFragmentSize",
			maxFragmentSize: 20,
			data:            []byte("12345678901\n012345678901\n1234567890"),
			start:           0,
			end:             35,
			expectedCount:   3,
			checkFragments: func(t *testing.T, fragments []*document.Fragment) {
				if len(fragments) >= 3 {
					if fragments[0].End != 12 { // After "12345678901\n"
						t.Errorf("Expected split at newline (position 12), got %d", fragments[0].End)
					}
					if fragments[1].End != 25 { // After "12345678901\n012345678901\n"
						t.Errorf("Expected split at newline (position 25), got %d", fragments[0].End)
					}
				}
			},
		},
		{
			name:            "Non-zero start position",
			maxFragmentSize: 10,
			data:            []byte("AAAAA\nBBBBB\nCCCCC\nDDDDD"),
			start:           6, // Start from the 'B'
			end:             23,
			expectedCount:   2,
			checkFragments: func(t *testing.T, fragments []*document.Fragment) {
				if len(fragments) >= 1 {
					if fragments[0].Start != 6 {
						t.Errorf("Expected start position to be 6, got %d", fragments[0].Start)
					}
				}
			},
		},
		{
			name:            "Exact maxFragmentSize",
			maxFragmentSize: 10,
			data:            []byte("1234567890"),
			start:           0,
			end:             10,
			expectedCount:   1,
			checkFragments:  nil,
		},
		{
			name:            "Empty data",
			maxFragmentSize: 10,
			data:            []byte{},
			start:           0,
			end:             0,
			expectedCount:   0,
			checkFragments:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			splitter := NewMarkdownSplitter(tc.maxFragmentSize)
			fragments := splitter.splitBySize(tc.data, tc.start, tc.end, "test.md")

			if len(fragments) != tc.expectedCount {
				t.Errorf("Expected %d fragments, got %d", tc.expectedCount, len(fragments))
			}

			// Run custom fragment checks if provided
			if tc.checkFragments != nil {
				tc.checkFragments(t, fragments)
			}

			// Check that fragments don't overlap and cover the entire range
			if len(fragments) > 0 {
				for i := 0; i < len(fragments)-1; i++ {
					if fragments[i].End != fragments[i+1].Start {
						t.Errorf("Fragment %d ends at %d but fragment %d starts at %d",
							i, fragments[i].End, i+1, fragments[i+1].Start)
					}
				}

				if fragments[0].Start != tc.start {
					t.Errorf("First fragment should start at %d, got %d", tc.start, fragments[0].Start)
				}

				if fragments[len(fragments)-1].End != tc.end {
					t.Errorf("Last fragment should end at %d, got %d", tc.end, fragments[len(fragments)-1].End)
				}
			}
		})
	}
}

func TestIsSetextUnderline(t *testing.T) {
	testCases := []struct {
		name     string
		line     []byte
		expected bool
	}{
		{
			name:     "Valid equals underline",
			line:     []byte("============="),
			expected: true,
		},
		{
			name:     "Valid dash underline",
			line:     []byte("-------------"),
			expected: true,
		},
		{
			name:     "Valid equals underline with leading spaces",
			line:     []byte("   ============="),
			expected: true,
		},
		{
			name:     "Valid dash underline with leading spaces",
			line:     []byte("   -------------"),
			expected: true,
		},
		{
			name:     "Too many leading spaces",
			line:     []byte("    ============="),
			expected: false,
		},
		{
			name:     "Too short",
			line:     []byte("=="),
			expected: false,
		},
		{
			name:     "Mixed characters",
			line:     []byte("===-==="),
			expected: false,
		},
		{
			name:     "Invalid character",
			line:     []byte("+++++++++"),
			expected: false,
		},
		{
			name:     "Empty line",
			line:     []byte{},
			expected: false,
		},
		{
			name:     "Only spaces",
			line:     []byte("   "),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isSetextUnderline(tc.line)
			if result != tc.expected {
				t.Errorf("Expected isSetextUnderline(%q) to be %v, got %v", tc.line, tc.expected, result)
			}
		})
	}
}

func TestIsBlank(t *testing.T) {
	testCases := []struct {
		name     string
		line     []byte
		expected bool
	}{
		{
			name:     "Empty line",
			line:     []byte{},
			expected: true,
		},
		{
			name:     "Only spaces",
			line:     []byte("   "),
			expected: true,
		},
		{
			name:     "Only tabs",
			line:     []byte("\t\t"),
			expected: true,
		},
		{
			name:     "Mixed whitespace",
			line:     []byte(" \t "),
			expected: true,
		},
		{
			name:     "Non-blank line",
			line:     []byte("Hello"),
			expected: false,
		},
		{
			name:     "Line with leading spaces",
			line:     []byte("   Hello"),
			expected: false,
		},
		{
			name:     "Line with trailing spaces",
			line:     []byte("Hello   "),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isBlank(tc.line)
			if result != tc.expected {
				t.Errorf("Expected isBlank(%q) to be %v, got %v", tc.line, tc.expected, result)
			}
		})
	}
}

func TestIsATXHeader(t *testing.T) {
	testCases := []struct {
		name     string
		line     []byte
		expected bool
	}{
		{
			name:     "Valid h1",
			line:     []byte("# Heading"),
			expected: true,
		},
		{
			name:     "Valid h2",
			line:     []byte("## Heading"),
			expected: true,
		},
		{
			name:     "Valid h3",
			line:     []byte("### Heading"),
			expected: true,
		},
		{
			name:     "Valid h4",
			line:     []byte("#### Heading"),
			expected: true,
		},
		{
			name:     "Valid h5",
			line:     []byte("##### Heading"),
			expected: true,
		},
		{
			name:     "Valid h6",
			line:     []byte("###### Heading"),
			expected: true,
		},
		{
			name:     "Valid h1 with leading spaces",
			line:     []byte("   # Heading"),
			expected: true,
		},
		{
			name:     "Too many leading spaces",
			line:     []byte("    # Heading"),
			expected: false,
		},
		{
			name:     "Too many hash characters",
			line:     []byte("####### Heading"),
			expected: false,
		},
		{
			name:     "No space after hash",
			line:     []byte("#Heading"),
			expected: false,
		},
		{
			name:     "Empty line",
			line:     []byte{},
			expected: false,
		},
		{
			name:     "Only hash characters",
			line:     []byte("###"),
			expected: true,
		},
		{
			name:     "Not a header",
			line:     []byte("This is not a header"),
			expected: false,
		},
		{
			name:     "Hash character in the middle",
			line:     []byte("This # is not a header"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isATXHeader(tc.line)
			if result != tc.expected {
				t.Errorf("Expected isATXHeader(%q) to be %v, got %v", tc.line, tc.expected, result)
			}
		})
	}
}
