package mem

import (
	"context"
	"time"
)

// WriterOpt is a no-op option in non-sqlite builds.
type WriterOpt func(*struct{})

// RunNamespaceWriter is a no-op in non-sqlite builds.
func (s *Store) RunNamespaceWriter(ctx context.Context, namespace string, opts ...WriterOpt) error {
	return nil
}

// RunNamespaceWriterUntilIdle is a no-op in non-sqlite builds.
func (s *Store) RunNamespaceWriterUntilIdle(ctx context.Context, namespace string, idle time.Duration, opts ...WriterOpt) error {
	return nil
}
