package fs

import (
	"context"

	"github.com/viant/afs"
	"github.com/viant/afs/storage"
)

// afsService is a Service implemented using github.com/viant/afs
type afsService struct {
	svc afs.Service
}

// NewAFS constructs a Service backed by the default AFS service.
func NewAFS() Service {
	return &afsService{svc: afs.New()}
}

func (a *afsService) List(ctx context.Context, location string) ([]storage.Object, error) {
	return a.svc.List(ctx, location)
}

func (a *afsService) Download(ctx context.Context, object storage.Object) ([]byte, error) {
	return a.svc.Download(ctx, object)
}
