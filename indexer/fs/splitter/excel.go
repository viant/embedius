package splitter

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/embedius/document"
	"github.com/xuri/excelize/v2"
)

// excelSplitter extracts text from Excel files and chunks by size while
// repeating the header rows for each fragment.
type excelSplitter struct {
	maxChunk int
}

// NewExcelSplitter returns a Splitter that extracts text from Excel and chunks
// by size, repeating header rows in each fragment.
func NewExcelSplitter(maxChunk int) Splitter {
	if maxChunk <= 0 {
		maxChunk = 4096
	}
	return &excelSplitter{maxChunk: maxChunk}
}

func (s *excelSplitter) Split(data []byte, metadata map[string]interface{}) []*document.Fragment {
	frags, _ := s.SplitWithContent(data, metadata)
	return frags
}

func (s *excelSplitter) SplitWithContent(data []byte, metadata map[string]interface{}) ([]*document.Fragment, []byte) {
	if len(data) == 0 {
		return nil, nil
	}
	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return NewSizeSplitter(s.maxChunk).Split(data, metadata), data
	}
	defer func() { _ = f.Close() }()

	path, _ := metadata["path"].(string)
	var fragments []*document.Fragment
	var full bytes.Buffer

	for _, sheet := range f.GetSheetList() {
		rows, err := f.GetRows(sheet)
		if err != nil || len(rows) == 0 {
			continue
		}
		header := rows[0]
		headerLine := buildHeaderLine(sheet, header)
		hasData := len(rows) > 1

		chunk := bytes.NewBuffer(nil)
		chunkRowStart := 1
		chunkRowEnd := 1
		resetChunk := func() {
			chunk.Reset()
			chunk.WriteString(headerLine)
			chunk.WriteByte('\n')
		}
		resetChunk()

		if !hasData {
			start := full.Len()
			full.Write(chunk.Bytes())
			end := full.Len()
			fragments = append(fragments, newExcelFragment(start, end, path, sheet, chunkRowStart, chunkRowEnd, full.Bytes()[start:end]))
			continue
		}

		for i := 1; i < len(rows); i++ {
			rowIdx := i + 1 // Excel rows are 1-based
			rowLine := buildRowLine(f, sheet, rowIdx, header, rows[i])
			if chunk.Len()+len(rowLine)+1 > s.maxChunk && chunk.Len() > 0 {
				start := full.Len()
				full.Write(chunk.Bytes())
				end := full.Len()
				fragments = append(fragments, newExcelFragment(start, end, path, sheet, chunkRowStart, chunkRowEnd, full.Bytes()[start:end]))
				resetChunk()
				chunkRowStart = rowIdx
			}
			chunk.WriteString(rowLine)
			chunk.WriteByte('\n')
			chunkRowEnd = rowIdx
		}
		if chunk.Len() > 0 {
			start := full.Len()
			full.Write(chunk.Bytes())
			end := full.Len()
			fragments = append(fragments, newExcelFragment(start, end, path, sheet, chunkRowStart, chunkRowEnd, full.Bytes()[start:end]))
		}
	}

	return fragments, full.Bytes()
}

func buildHeaderLine(sheet string, header []string) string {
	var b strings.Builder
	b.WriteString("Sheet: ")
	b.WriteString(sheet)
	b.WriteString("\nHeader: ")
	for i, h := range header {
		if i > 0 {
			b.WriteString("\t")
		}
		b.WriteString(h)
	}
	return b.String()
}

func buildRowLine(f *excelize.File, sheet string, rowIdx int, header []string, row []string) string {
	maxCols := len(header)
	if len(row) > maxCols {
		maxCols = len(row)
	}
	var b strings.Builder
	b.WriteString("Row ")
	b.WriteString(strconv.Itoa(rowIdx))
	b.WriteString(": ")
	for col := 1; col <= maxCols; col++ {
		if col > 1 {
			b.WriteString("\t")
		}
		cellRef, _ := excelize.CoordinatesToCellName(col, rowIdx)
		val := ""
		if col-1 < len(row) {
			val = row[col-1]
		}
		formula, _ := f.GetCellFormula(sheet, cellRef)
		if formula != "" {
			if val != "" {
				b.WriteString(val)
				b.WriteString(" (f=")
				b.WriteString(formula)
				b.WriteString(")")
			} else {
				b.WriteString("f=")
				b.WriteString(formula)
			}
		} else {
			b.WriteString(val)
		}
	}
	return b.String()
}

func newExcelFragment(start, end int, path, sheet string, rowStart, rowEnd int, data []byte) *document.Fragment {
	checksum := sha256.Sum256(data)
	checksumInt := int(binary.BigEndian.Uint32(checksum[:4]))
	meta := map[string]string{
		"path":       path,
		"sheet":      sheet,
		"start_row":  strconv.Itoa(rowStart),
		"end_row":    strconv.Itoa(rowEnd),
		"row_range":  fmt.Sprintf("%d-%d", rowStart, rowEnd),
		"header_row": "1",
	}
	return &document.Fragment{
		Start:    start,
		End:      end,
		Checksum: checksumInt,
		Kind:     "excel",
		Meta:     meta,
	}
}
