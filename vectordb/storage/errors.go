package storage

import "errors"

var (
	// ErrClosed is returned when the store has been closed.
	ErrClosed = errors.New("storage: store closed")

	// ErrInvalidPtr indicates the pointer does not reference a valid record.
	ErrInvalidPtr = errors.New("storage: invalid pointer")

	// ErrCorrupt indicates on-disk data corruption was detected.
	ErrCorrupt = errors.New("storage: data corruption detected")
)
