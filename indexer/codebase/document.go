package codebase

import (
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/embedius/vectordb/meta"
	info "github.com/viant/linager/inspector/graph"
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

	fragmentID := meta.GetString(d.Metadata, meta.FragmentID)
	part := 0
	if fragmentID != "" {
		index := strings.LastIndex(fragmentID, ":")
		if index > 0 {
			part, _ = strconv.Atoi(fragmentID[index+1:])
		}
	}
	return &info.Document{
		ID:        meta.GetString(d.Metadata, meta.DocumentID),
		Name:      meta.GetString(d.Metadata, "name"),
		Path:      meta.GetString(d.Metadata, meta.PathKey),
		Part:      part,
		Project:   meta.GetString(d.Metadata, "project"),
		Package:   meta.GetString(d.Metadata, "package"),
		Kind:      info.DocumentKind(meta.GetString(d.Metadata, "kind")),
		Signature: meta.GetString(d.Metadata, "signature"),
		Content:   d.PageContent,
	}
}

type Document info.Document

func (p *Document) Document() schema.Document {
	if p.Part > 0 {
		return schema.Document{
			PageContent: p.Content,
			Metadata: map[string]interface{}{
				meta.DocumentID:   p.ID,
				meta.FragmentID:   p.ID + ":" + strconv.Itoa(p.Part),
				meta.NameKey:      p.Name,
				meta.PathKey:      p.Path,
				meta.ProjectKey:   p.Project,
				meta.PackageKey:   p.Package,
				meta.KindKey:      string(p.Kind),
				meta.SignatureKey: p.Signature,
			},
		}
	}

	return schema.Document{
		PageContent: p.Content,
		Metadata: map[string]interface{}{
			meta.DocumentID:   p.ID,
			meta.NameKey:      p.Name,
			meta.PathKey:      p.Path,
			meta.ProjectKey:   p.Project,
			meta.PackageKey:   p.Package,
			meta.KindKey:      string(p.Kind),
			meta.SignatureKey: p.Signature,
		},
	}
}

// Helper function to safely extract a string from metadata
