package codebase

import (
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/linager/inspector/info"
)

// ProjectDocuments returns a list of projects with their matched documents
func (i *Codebase) ProjectDocuments(docs []schema.Document) []*Project {
	var projects = make(map[string]*Project)
	for _, doc := range docs {
		schemaDoc := SchemaDocument(doc)
		doc := schemaDoc.Document()
		project, ok := projects[doc.Project]
		if !ok {
			projectInfo, ok := i.ProjectInfoByName.Get(doc.Project)
			if !ok {
				continue
			}
			project = &Project{
				Info:    projectInfo,
				Matched: info.Documents{},
			}
			projects[doc.Project] = project
		}
		project.Matched = append(project.Matched, doc)
	}
	var results []*Project
	for _, project := range projects {
		if len(project.Matched) == 0 {
			continue
		}
		project.Grouped = project.Matched.GroupBy()
		results = append(results, project)
	}
	return results
}
