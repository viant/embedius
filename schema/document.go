package schema

// Document represents a text chunk with optional metadata and score.
// It mirrors the minimal shape used across this repository.
type Document struct {
	PageContent string                 `json:"page_content"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	// Score is optional and populated by similarity search.
	Score float32 `json:"score,omitempty"`
}
