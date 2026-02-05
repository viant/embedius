package splitter

import (
	"archive/zip"
	"bytes"
	"testing"
)

func TestDOCXSplitter_Split(t *testing.T) {
	data := buildDOCX(t, `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>Hello</w:t></w:r></w:p></w:body></w:document>`)
	s := NewDOCXSplitter(64)
	frags := s.Split(data, map[string]interface{}{"path": "dummy.docx"})
	if len(frags) == 0 {
		t.Fatalf("expected fragments, got 0")
	}
}

func buildDOCX(t *testing.T, documentXML string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatalf("create zip entry: %v", err)
	}
	if _, err := w.Write([]byte(documentXML)); err != nil {
		t.Fatalf("write document.xml: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
	return buf.Bytes()
}
