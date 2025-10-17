package memstore

import (
	vstorage "github.com/viant/embedius/vectordb/storage"
	"sync"
)

// Store is a simple in-memory implementation of ValueStore.
// It is intended for testing and in-memory Phase 2 wiring only.
type Store struct {
	mu     sync.RWMutex
	data   []byte
	stats  vstorage.Stats
	closed bool
}

// New creates a new in-memory value store.
func New() *Store {
	return &Store{}
}

// Append appends value to the in-memory buffer and returns a pointer.
func (s *Store) Append(value []byte) (vstorage.Ptr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return vstorage.Ptr{}, vstorage.ErrClosed
	}
	off := len(s.data)
	s.data = append(s.data, value...)
	s.stats.Appends++
	s.stats.BytesWritten += uint64(len(value))
	// single implicit segment 0
	return vstorage.Ptr{SegmentID: 0, Offset: uint64(off), Length: uint32(len(value))}, nil
}

// Read returns a copy of the bytes referenced by ptr.
func (s *Store) Read(ptr vstorage.Ptr) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, vstorage.ErrClosed
	}
	if ptr.SegmentID != 0 {
		return nil, vstorage.ErrInvalidPtr
	}
	start := int(ptr.Offset)
	end := start + int(ptr.Length)
	if start < 0 || end > len(s.data) || start > end {
		return nil, vstorage.ErrInvalidPtr
	}
	out := make([]byte, int(ptr.Length))
	copy(out, s.data[start:end])
	s.stats.BytesRead += uint64(len(out))
	return out, nil
}

// Delete is a no-op tombstone in-memory; it validates the ptr.
func (s *Store) Delete(ptr vstorage.Ptr) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return vstorage.ErrClosed
	}
	if ptr.SegmentID != 0 {
		return vstorage.ErrInvalidPtr
	}
	start := int(ptr.Offset)
	end := start + int(ptr.Length)
	if start < 0 || end > len(s.data) || start > end {
		return vstorage.ErrInvalidPtr
	}
	return nil
}

// Sync is a no-op for in-memory store.
func (s *Store) Sync() error { return nil }

// Close marks the store as closed. Further ops return ErrClosed.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.data = nil
	return nil
}

// Stats returns current stats snapshot.
func (s *Store) Stats() vstorage.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := s.stats
	st.Segments = 1
	return st
}

var _ vstorage.ValueStore = (*Store)(nil)
