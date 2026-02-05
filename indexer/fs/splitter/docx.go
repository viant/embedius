package splitter

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"io"
	"strings"

	"github.com/viant/embedius/document"
)

// docxSplitter extracts text from DOCX files using a pure Go parser.
type docxSplitter struct {
	delegate Splitter
}

// NewDOCXSplitter returns a Splitter that extracts text from DOCX and delegates
// to a size-based splitter for chunking.
func NewDOCXSplitter(maxChunk int) Splitter {
	if maxChunk <= 0 {
		maxChunk = 4096
	}
	return &docxSplitter{delegate: NewSizeSplitter(maxChunk)}
}

func (d *docxSplitter) Split(data []byte, metadata map[string]interface{}) []*document.Fragment {
	text := extractDOCXText(data)
	if len(text) == 0 {
		text = extractPrintableText(data)
	}
	return d.delegate.Split(text, metadata)
}

func extractDOCXText(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	var docFile *zip.File
	for _, f := range r.File {
		if strings.EqualFold(f.Name, "word/document.xml") {
			docFile = f
			break
		}
	}
	if docFile == nil {
		return nil
	}
	rc, err := docFile.Open()
	if err != nil {
		return nil
	}
	defer rc.Close()
	return extractDOCXTextFromXML(rc)
}

func extractDOCXTextFromXML(r io.Reader) []byte {
	dec := xml.NewDecoder(r)
	var buf bytes.Buffer
	var lastWasNewline bool
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "t", "instrText":
				var text string
				if err := dec.DecodeElement(&text, &t); err == nil {
					buf.WriteString(text)
					lastWasNewline = false
				}
			case "tab":
				buf.WriteByte('\t')
				lastWasNewline = false
			case "br", "cr":
				buf.WriteByte('\n')
				lastWasNewline = true
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "p":
				if !lastWasNewline {
					buf.WriteByte('\n')
					lastWasNewline = true
				}
			case "tr":
				if !lastWasNewline {
					buf.WriteByte('\n')
					lastWasNewline = true
				}
			case "tc":
				if !lastWasNewline {
					buf.WriteByte('\t')
					lastWasNewline = false
				}
			}
		}
	}
	return buf.Bytes()
}
