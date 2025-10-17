package mem

import "errors"

var (
	// ErrNamespaceLocked indicates the namespace is already locked by another writer.
	ErrNamespaceLocked = errors.New("mem: namespace locked by another writer")
	// ErrNamespaceLockTimeout indicates lock acquisition timed out.
	ErrNamespaceLockTimeout = errors.New("mem: namespace lock timeout")
	// ErrIndexCorrupt indicates the on-disk index file is malformed or inconsistent.
	ErrIndexCorrupt = errors.New("mem: index file corrupt")
)
