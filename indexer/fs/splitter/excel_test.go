package splitter

import (
	"bytes"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestExcelSplitter_IncludesHeaderPerChunk(t *testing.T) {
	f := excelize.NewFile()
	sheet := f.GetSheetName(0)
	if err := f.SetSheetRow(sheet, "A1", &[]interface{}{"col1", "col2"}); err != nil {
		t.Fatalf("set header: %v", err)
	}
	for i := 2; i <= 8; i++ {
		row := []interface{}{"v" + string(rune('A'+i)), i}
		cell, _ := excelize.CoordinatesToCellName(1, i)
		if err := f.SetSheetRow(sheet, cell, &row); err != nil {
			t.Fatalf("set row: %v", err)
		}
	}
	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		t.Fatalf("write xlsx: %v", err)
	}

	s := NewExcelSplitter(80)
	cs, ok := s.(ContentSplitter)
	if !ok {
		t.Fatalf("expected ContentSplitter")
	}
	fragments, content := cs.SplitWithContent(buf.Bytes(), map[string]interface{}{"path": "test.xlsx"})
	if len(fragments) < 2 {
		t.Fatalf("expected multiple fragments, got %d", len(fragments))
	}
	if len(content) == 0 {
		t.Fatalf("expected extracted content")
	}
	text := string(content)
	for i, frag := range fragments {
		if frag.End > len(content) || frag.Start < 0 || frag.End <= frag.Start {
			t.Fatalf("fragment %d out of bounds", i)
		}
		fragText := text[frag.Start:frag.End]
		if !strings.Contains(fragText, "Header:") {
			t.Fatalf("fragment %d missing header", i)
		}
		if frag.End-frag.Start > 80 {
			t.Fatalf("fragment %d exceeds max chunk", i)
		}
	}
}
