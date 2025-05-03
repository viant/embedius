package codebase

import (
	info "github.com/viant/linager/inspector/graph"
	"github.com/viant/linager/inspector/repository"
)

type Project struct {
	Info    *repository.Project
	Matched info.Documents
	Grouped info.Documents
}
