package mcp

import "github.com/viant/embedius/service"

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
