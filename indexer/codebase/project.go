package codebase

import (
	"github.com/viant/linager/inspector/info"
	"github.com/viant/linager/inspector/repository"
)

type Project struct {
	Info    *repository.Project
	Matched info.Documents
	Grouped info.Documents
}
