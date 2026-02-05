package mcp

import (
	"time"

	"github.com/viant/embedius/matching/option"
	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/service"
	"github.com/viant/mcp-protocol/extension"
)

type SearchInput struct {
	Root     string  `json:"root"`
	Query    string  `json:"query"`
	Limit    int     `json:"limit,omitempty"`
	MinScore float64 `json:"min_score,omitempty"`
	Model    string  `json:"model,omitempty"`
}

type SearchOutput struct {
	Results []service.SearchResult `json:"results"`
}

type RootsInput struct {
	Root string `json:"root,omitempty"`
}

type RootsOutput struct {
	Roots []service.RootInfo `json:"roots"`
}

// MatchInput mirrors agently resources.match input.
type MatchInput struct {
	Query string `json:"query"`
	// RootURI/Roots are retained for backward compatibility.
	RootURI []string `json:"rootUri,omitempty"`
	Roots   []string `json:"roots,omitempty"`
	RootIDs []string `json:"rootIds,omitempty"`
	Path    string   `json:"path,omitempty"`
	Model   string   `json:"model,omitempty"`

	MaxDocuments int             `json:"maxDocuments,omitempty"`
	IncludeFile  bool            `json:"includeFile,omitempty"`
	Match        *option.Options `json:"match,omitempty"`

	LimitBytes int `json:"limitBytes,omitempty"`
	Cursor     int `json:"cursor,omitempty"`
}

// MatchOutput mirrors agently AugmentDocsOutput with extra fields.
type MatchOutput struct {
	Content       string            `json:"content,omitempty"`
	Documents     []schema.Document `json:"documents,omitempty"`
	DocumentsSize int               `json:"documentsSize,omitempty"`
	NextCursor    int               `json:"nextCursor,omitempty"`
	Cursor        int               `json:"cursor,omitempty"`
	LimitBytes    int               `json:"limitBytes,omitempty"`
	SystemContent string            `json:"systemContent,omitempty"`
	DocumentRoots map[string]string `json:"documentRoots,omitempty"`
}

// ListInput mirrors agently resources.list input.
type ListInput struct {
	RootURI   string   `json:"root,omitempty"`
	RootID    string   `json:"rootId,omitempty"`
	Path      string   `json:"path,omitempty"`
	Recursive bool     `json:"recursive,omitempty"`
	Include   []string `json:"include,omitempty"`
	Exclude   []string `json:"exclude,omitempty"`
	MaxItems  int      `json:"maxItems,omitempty"`
}

type ListItem struct {
	URI      string    `json:"uri"`
	Path     string    `json:"path"`
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified"`
	RootID   string    `json:"rootId,omitempty"`
}

type ListOutput struct {
	Items []ListItem `json:"items"`
	Total int        `json:"total"`
}

type BytesRange struct {
	OffsetBytes int64 `json:"offsetBytes,omitempty"`
	LengthBytes int   `json:"lengthBytes,omitempty"`
}

type LineRange struct {
	StartLine int `json:"startLine,omitempty"`
	LineCount int `json:"lineCount,omitempty"`
}

// ReadInput mirrors agently resources.read input.
type ReadInput struct {
	RootURI string `json:"root,omitempty"`
	RootID  string `json:"rootId,omitempty"`
	Path    string `json:"path,omitempty"`
	URI     string `json:"uri,omitempty"`

	BytesRange BytesRange `json:"bytesRange,omitempty"`
	LineRange

	MaxBytes int    `json:"maxBytes,omitempty"`
	Mode     string `json:"mode,omitempty"`
}

// ReadOutput mirrors agently resources.read output.
type ReadOutput struct {
	URI     string `json:"uri"`
	Path    string `json:"path"`
	Content string `json:"content"`
	Size    int    `json:"size"`

	Returned  int `json:"returned,omitempty"`
	Remaining int `json:"remaining,omitempty"`

	StartLine int `json:"startLine,omitempty"`
	EndLine   int `json:"endLine,omitempty"`

	Binary       bool                    `json:"binary,omitempty"`
	ModeApplied  string                  `json:"modeApplied,omitempty"`
	Continuation *extension.Continuation `json:"continuation,omitempty"`
}

// GrepInput mirrors agently resources.grepFiles input.
type GrepInput struct {
	Pattern        string   `json:"pattern"`
	ExcludePattern string   `json:"excludePattern,omitempty"`
	RootURI        string   `json:"root,omitempty"`
	RootID         string   `json:"rootId,omitempty"`
	Path           string   `json:"path"`
	Recursive      bool     `json:"recursive,omitempty"`
	Include        []string `json:"include,omitempty"`
	Exclude        []string `json:"exclude,omitempty"`

	CaseInsensitive bool `json:"caseInsensitive,omitempty"`

	Mode      string `json:"mode,omitempty"`
	Bytes     int    `json:"bytes,omitempty"`
	Lines     int    `json:"lines,omitempty"`
	MaxFiles  int    `json:"maxFiles,omitempty"`
	MaxBlocks int    `json:"maxBlocks,omitempty"`

	SkipBinary  bool `json:"skipBinary,omitempty"`
	MaxSize     int  `json:"maxSize,omitempty"`
	Concurrency int  `json:"concurrency,omitempty"`
}

type GrepStats struct {
	Scanned   int  `json:"scanned"`
	Matched   int  `json:"matched"`
	Truncated bool `json:"truncated"`
}

type GrepSnippet struct {
	StartLine   int      `json:"startLine,omitempty"`
	EndLine     int      `json:"endLine,omitempty"`
	OffsetBytes int64    `json:"offsetBytes,omitempty"`
	LengthBytes int      `json:"lengthBytes,omitempty"`
	Text        string   `json:"text,omitempty"`
	Hits        [][2]int `json:"hits,omitempty"`
	Cut         bool     `json:"cut,omitempty"`
}

type GrepFile struct {
	Path       string        `json:"path"`
	URI        string        `json:"uri"`
	SearchHash string        `json:"searchHash,omitempty"`
	RangeKey   string        `json:"rangeKey,omitempty"`
	Matches    int           `json:"matches,omitempty"`
	Score      float32       `json:"score,omitempty"`
	Snippets   []GrepSnippet `json:"snippets,omitempty"`
	Omitted    int           `json:"omitted,omitempty"`
}

type GrepOutput struct {
	Stats GrepStats  `json:"stats"`
	Files []GrepFile `json:"files,omitempty"`
}
