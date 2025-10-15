package fs

import (
	"context"
	"github.com/viant/afs/storage"
)

// Service abstracts listing and downloading objects so we can
// support multiple backends (e.g. local/remote FS, MCP resources).
type Service interface {
	// List returns objects available at the given location/URI.
	List(ctx context.Context, location string) ([]storage.Object, error)
	// Download returns the content of the given object.
	Download(ctx context.Context, object storage.Object) ([]byte, error)
}
