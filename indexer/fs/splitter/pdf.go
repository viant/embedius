package splitter

import (
	"bytes"
	"io"
	"unicode/utf8"

	"github.com/ledongthuc/pdf"
	"github.com/viant/embedius/document"
)

// pdfSplitter is a lightweight splitter for PDF files that attempts to
// extract printable text without requiring external PDF libraries.
// It is intentionally simple to avoid new dependencies.
type pdfSplitter struct {
	delegate Splitter
}

// NewPDFSplitter returns a Splitter that extracts printable text and delegates
// to a size-based splitter for chunking.
func NewPDFSplitter(maxChunk int) Splitter {
	if maxChunk <= 0 {
		maxChunk = 4096
	}
	return &pdfSplitter{delegate: NewSizeSplitter(maxChunk)}
}

func (p *pdfSplitter) Split(data []byte, metadata map[string]interface{}) []*document.Fragment {
	frags, _ := p.SplitWithContent(data, metadata)
	return frags
}

func (p *pdfSplitter) SplitWithContent(data []byte, metadata map[string]interface{}) ([]*document.Fragment, []byte) {
	text := extractPDFText(data)
	return p.delegate.Split(text, metadata), text
}

func extractPDFText(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	if r, err := pdf.NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
		if reader, err := r.GetPlainText(); err == nil {
			if out, err := io.ReadAll(reader); err == nil && len(out) > 0 {
				return out
			}
		}
	}
	return extractPrintableText(data)
}

func extractPrintableText(in []byte) []byte {
	var out bytes.Buffer
	for len(in) > 0 {
		r, size := utf8.DecodeRune(in)
		if r == utf8.RuneError && size == 1 {
			b := in[0]
			if isPrintableASCII(b) {
				out.WriteByte(b)
			}
			in = in[1:]
			continue
		}
		in = in[size:]
		if isPrintableRune(r) {
			out.WriteRune(r)
		}
	}
	return out.Bytes()
}

func isPrintableASCII(b byte) bool {
	return b == '\n' || b == '\r' || b == '\t' || (b >= 32 && b < 127)
}

func isPrintableRune(r rune) bool {
	if r == '\n' || r == '\r' || r == '\t' {
		return true
	}
	if r >= 32 && r < 127 {
		return true
	}
	if r >= 127 && r <= 0x10FFFF {
		return true
	}
	return false
}
