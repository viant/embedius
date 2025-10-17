package document

import (
	"fmt"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectordb/meta"
)

// Fragments represents a collection of document fragments
type Fragments []*Fragment

// ByFragmentID returns a map of fragments indexed by their fragment ID
func (f Fragments) ByFragmentID(docID string) map[string]*Fragment {
	var result = make(map[string]*Fragment)
	for _, fragment := range f {
		id, ok := fragment.Meta[meta.FragmentID]
		if !ok {
			id = fragment.ID(docID)
		}
		result[id] = fragment
	}
	return result
}

// VectorDBIDs returns a slice of VectorDBIDs from the fragments
func (f Fragments) VectorDBIDs() []string {
	ids := make([]string, 0, len(f))
	for _, fragment := range f {
		if fragment.VectorDBID != "" {
			ids = append(ids, fragment.VectorDBID)
		}
	}
	return ids
}

// Fragment represents a portion of a document
type Fragment struct {
	Start      int               `json:"start"`
	End        int               `json:"end"`
	Checksum   int               `json:"checksum"`
	VectorDBID string            `json:"entryId,omitempty"`
	Kind       string            `json:"kind,omitempty"`
	Name       string            `json:"name,omitempty"`
	Meta       map[string]string `json:"meta,omitempty"`
}

func (f *Fragment) ID(docID string) string {
	return fmt.Sprintf("%s:%d-%d", docID, f.Start, f.End)
}

// NewDocument creates a schema.Document from this fragment
func (f *Fragment) NewDocument(path string, content []byte) schema.Document {
	// Ensure we're within bounds
	if f.Start >= len(content) {
		f.Start = 0
	}
	if f.End > len(content) {
		f.End = len(content)
	}

	// Extract the fragment content
	fragmentContent := ""
	if f.End > f.Start {
		fragmentContent = string(content[f.Start:f.End])
	}

	metadata := map[string]interface{}{
		meta.PathKey:    path,
		"start":         f.Start,
		"end":           f.End,
		"checksum":      f.Checksum,
		meta.DocumentID: path,
		meta.FragmentID: f.ID(path),
	}

	// Add metadata from the fragment
	if f.Meta != nil {
		for k, v := range f.Meta {
			metadata[k] = v
		}
	}

	// Use the kind if available
	if f.Kind != "" {
		metadata["kind"] = f.Kind
	}

	// Use the name if available
	if f.Name != "" {
		metadata["name"] = f.Name
	}

	return schema.Document{
		PageContent: fragmentContent,
		Metadata:    metadata,
	}
}
