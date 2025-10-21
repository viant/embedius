package splitter

import (
	"bytes"
	"testing"
)

// Test that large files use a size-based splitter honoring the default chunk size (4096)
func TestLargeFileUsesDefaultChunkSize(t *testing.T) {
	f := NewFactory(4096)

	// Simulate a large file by passing fileSize > 1MB so the factory selects sizeSplitter
	s := f.GetSplitter("big.bin", 2*1024*1024)

	// Prepare a small in-memory payload; SizeSplitter splits purely by size, not by fileSize
	data := bytes.Repeat([]byte("a"), 9000)
	fr := s.Split(data, map[string]interface{}{"path": "big.bin"})

	if len(fr) != 3 {
		t.Fatalf("expected 3 fragments, got %d", len(fr))
	}

	// Expect two full 4096-sized chunks and a remainder
	if got := fr[0].End - fr[0].Start; got != 4096 {
		t.Fatalf("expected first fragment size 4096, got %d", got)
	}
	if got := fr[1].End - fr[1].Start; got != 4096 {
		t.Fatalf("expected second fragment size 4096, got %d", got)
	}
	if got := fr[2].End - fr[2].Start; got != 808 {
		t.Fatalf("expected third fragment size 808, got %d", got)
	}
}
