package mmapstore

import (
	"os"
	"path/filepath"
	"testing"
)

func tmpDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestStore_AppendRead(t *testing.T) {
	base := tmpDir(t)
	s, err := Open(Options{BasePath: base})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	data := []byte("hello world")
	ptr, err := s.Append(data)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	got, err := s.Read(ptr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestStore_ReopenAndRecover(t *testing.T) {
	base := tmpDir(t)
	s, err := Open(Options{BasePath: base})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	p1, err := s.Append([]byte("first"))
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	p2, err := s.Append([]byte("second"))
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Inject trailing garbage to active segment to simulate crash.
	activePath := filepath.Join(base, "data_000000.vdat")
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	// Write a few bytes that cannot form a valid record.
	if _, err := f.Write([]byte{0x01, 0xff, 0xff, 0xff}); err != nil {
		t.Fatalf("inject junk: %v", err)
	}
	_ = f.Close()

	// Close and reopen to trigger recovery scan.
	_ = s.Close()
	s2, err := Open(Options{BasePath: base})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	g1, err := s2.Read(p1)
	if err != nil || string(g1) != "first" {
		t.Fatalf("read p1: %v, got %q", err, g1)
	}
	g2, err := s2.Read(p2)
	if err != nil || string(g2) != "second" {
		t.Fatalf("read p2: %v, got %q", err, g2)
	}
}

func TestStore_Delete(t *testing.T) {
	base := tmpDir(t)
	s, err := Open(Options{BasePath: base})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	p, err := s.Append([]byte("to-delete"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := s.Delete(p); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Delete does not prevent reading the original payload in this design.
	if b, err := s.Read(p); err != nil || string(b) != "to-delete" {
		t.Fatalf("read after delete: %v, got %q", err, b)
	}
}
