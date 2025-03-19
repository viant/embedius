package codebase

import (
	"context"
	"fmt"
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/embedius/document"
	"github.com/viant/embedius/indexer/cache"
	"github.com/viant/linager/inspector"
	"github.com/viant/linager/inspector/info"
	"github.com/viant/linager/inspector/repository"
	"strconv"
	"strings"
	"time"
)

// Indexer implements indexing for codebase projects
type Indexer struct {
	ProjectInfoByName *cache.Map[string, repository.Project]
	ProjectInfo       *cache.Map[string, repository.Project]
	Project           *cache.Map[string, info.Project]
	embeddingsModel   string
	detector          *repository.Detector
}

// LookupProject returns project for the specified URI
func (i *Indexer) LookupProject(URI string) (*info.Project, error) {
	project, ok := i.Project.Get(URI)
	if !ok {
		return nil, fmt.Errorf("failed to find project for URI: %s", URI)
	}
	return project, nil
}

// Namespace returns a namespace for the specified URI
func (i *Indexer) Namespace(ctx context.Context, URI string) (string, error) {
	projectRepo, err := i.getProjectInfo(URI)
	if err != nil {
		return "", err
	}
	projectName := projectRepo.Name
	if projectName == "" {
		projectName = projectRepo.RootPath
	}
	projectFragment := strings.ReplaceAll(strings.ReplaceAll(projectRepo.Name, ".", "_"), "/", "_")
	embeddingsHash, err := cache.Hash([]byte(i.embeddingsModel))
	if err != nil {
		return "", fmt.Errorf("failed to hash embedder: %w", err)
	}
	return strconv.Itoa(int(embeddingsHash)) + "_" + projectFragment + "_" + projectRepo.Type, nil
}

func (i *Indexer) getProjectInfo(URI string) (*repository.Project, error) {
	project, ok := i.ProjectInfo.Get(URI)
	if !ok {
		var err error
		project, err = i.detector.DetectProject(URI)
		if err != nil {
			return nil, err
		}
		i.ProjectInfo.Set(URI, project)
		i.ProjectInfoByName.Set(project.Name, project)
	}
	return project, nil
}

// Index indexes a codebase project
func (i *Indexer) Index(ctx context.Context, URI string, cache *cache.Map[string, document.Entry]) ([]schema.Document, []string, error) {
	projectInfo, err := i.getProjectInfo(URI)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get project repository: %w", err)
	}
	projectFactory := inspector.NewFactory(info.DefaultConfig())
	project, err := projectFactory.InspectProject(projectInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to inspect project: %w", err)
	}

	documents, err := project.CreateDocuments(ctx, projectInfo.RelativePath)
	if err != nil {
		return nil, nil, err
	}

	var schemaDocuments []schema.Document
	var idsToRemove []string
	for _, doc := range documents {
		prev, _ := cache.Get(doc.GetID())
		// Skip documents that haven't changed
		if prev != nil && prev.Hash == doc.Hash {
			continue
		}
		// Collect IDs to remove from previous version
		if prev != nil {
			idsToRemove = append(idsToRemove, prev.Fragments.VectorDBIDs()...)
		}

		// Create new entry
		entry := &document.Entry{
			ID:      doc.GetID(),
			ModTime: time.Now(),
			Hash:    doc.Hash,
			Fragments: []*document.Fragment{{
				Meta: map[string]string{
					document.DocumentID: doc.GetID(),
				}}}}

		cache.Set(doc.GetID(), entry)
		projectDocument := Document(*doc)
		schemaDocuments = append(schemaDocuments, projectDocument.Document())
	}
	return schemaDocuments, idsToRemove, nil
}

// New creates a new Indexer instance
func New(embeddingsModel string) *Indexer {
	return &Indexer{
		detector:          repository.New(),
		ProjectInfo:       cache.NewMap[string, repository.Project](),
		Project:           cache.NewMap[string, info.Project](),
		ProjectInfoByName: cache.NewMap[string, repository.Project](),
		embeddingsModel:   embeddingsModel,
	}
}
