package splitter

import "testing"

func TestPDFSplitter_Split(t *testing.T) {
	s := NewPDFSplitter(64)
	data := []byte("%PDF-1.4\nHello\nWorld\n%%EOF")
	frags := s.Split(data, map[string]interface{}{"path": "dummy.pdf"})
	if len(frags) == 0 {
		t.Fatalf("expected fragments, got 0")
	}
}
