package codebase

import (
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/embedius/document"
	"github.com/viant/linager/inspector/info"
	"strconv"
	"strings"
)

type SchemaDocument schema.Document

type SchemaDocuments []schema.Document

func (d SchemaDocuments) InfoDocuments() map[string][]*info.Document {
	var results = make(map[string][]*info.Document)
	for i := range d {
		schemaDoc := SchemaDocument(d[i])
		doc := schemaDoc.Document()
		project := doc.Project
		results[project] = append(results[project], doc)
	}
	return results
}

func (d *SchemaDocument) Document() *info.Document {

	fragmentID := getStringFromMetadata(d.Metadata, document.FragmentID)
	part := 0
	if fragmentID != "" {
		index := strings.LastIndex(fragmentID, ":")
		if index > 0 {
			part, _ = strconv.Atoi(fragmentID[index+1:])
		}
	}
	return &info.Document{
		ID:        getStringFromMetadata(d.Metadata, document.DocumentID),
		Name:      getStringFromMetadata(d.Metadata, "name"),
		Path:      getStringFromMetadata(d.Metadata, "path"),
		Part:      part,
		Project:   getStringFromMetadata(d.Metadata, "project"),
		Package:   getStringFromMetadata(d.Metadata, "package"),
		Kind:      info.DocumentKind(getStringFromMetadata(d.Metadata, "kind")),
		Signature: getStringFromMetadata(d.Metadata, "signature"),
		Content:   d.PageContent,
	}
}

type Document info.Document

func (p *Document) Document() schema.Document {
	if p.Part > 0 {
		return schema.Document{
			PageContent: p.Content,
			Metadata: map[string]interface{}{
				document.DocumentID: p.ID,
				document.FragmentID: p.ID + ":" + strconv.Itoa(p.Part),
				"name":              p.Name,
				"path":              p.Path,
				"project":           p.Project,
				"package":           p.Package,
				"kind":              string(p.Kind),
				"signature":         p.Signature,
			},
		}
	}

	return schema.Document{
		PageContent: p.Content,
		Metadata: map[string]interface{}{
			document.DocumentID: p.ID,
			"name":              p.Name,
			"path":              p.Path,
			"project":           p.Project,
			"package":           p.Package,
			"kind":              string(p.Kind),
			"signature":         p.Signature,
		},
	}
}

// Helper function to safely extract a string from metadata
func getStringFromMetadata(metadata map[string]any, key string) string {
	if value, ok := metadata[key]; ok {
		text, _ := value.(string)
		return text
	}
	return ""
}
