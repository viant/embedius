package splitter

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/shakinm/xlsReader/xls"
	"github.com/shakinm/xlsReader/xls/structure"
	"github.com/viant/embedius/document"
)

// xlsSplitter extracts text from XLS files and chunks by size while
// repeating the header rows for each fragment.
type xlsSplitter struct {
	maxChunk int
}

// NewXLSSplitter returns a Splitter that extracts text from XLS and chunks
// by size, repeating header rows in each fragment.
func NewXLSSplitter(maxChunk int) Splitter {
	if maxChunk <= 0 {
		maxChunk = 4096
	}
	return &xlsSplitter{maxChunk: maxChunk}
}

func (s *xlsSplitter) Split(data []byte, metadata map[string]interface{}) []*document.Fragment {
	frags, _ := s.SplitWithContent(data, metadata)
	return frags
}

func (s *xlsSplitter) SplitWithContent(data []byte, metadata map[string]interface{}) ([]*document.Fragment, []byte) {
	if len(data) == 0 {
		return nil, nil
	}
	wb, err := xls.OpenReader(bytes.NewReader(data))
	if err != nil {
		return NewSizeSplitter(s.maxChunk).Split(data, metadata), data
	}

	path, _ := metadata["path"].(string)
	var fragments []*document.Fragment
	var full bytes.Buffer

	for i := 0; i < wb.GetNumberSheets(); i++ {
		sheet, err := wb.GetSheet(i)
		if err != nil || sheet == nil {
			continue
		}
		rows := sheet.GetRows()
		if len(rows) == 0 {
			continue
		}
		header := xlsRowValues(rows[0].GetCols())
		headerLine := buildHeaderLine(sheet.GetName(), header)
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
			fragments = append(fragments, newXLSFragment(start, end, path, sheet.GetName(), chunkRowStart, chunkRowEnd, full.Bytes()[start:end]))
			continue
		}

		for r := 1; r < len(rows); r++ {
			rowIdx := r + 1 // 1-based
			rowLine := buildRowLineFromValues(rowIdx, header, xlsRowValues(rows[r].GetCols()))
			if chunk.Len()+len(rowLine)+1 > s.maxChunk && chunk.Len() > 0 {
				start := full.Len()
				full.Write(chunk.Bytes())
				end := full.Len()
				fragments = append(fragments, newXLSFragment(start, end, path, sheet.GetName(), chunkRowStart, chunkRowEnd, full.Bytes()[start:end]))
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
			fragments = append(fragments, newXLSFragment(start, end, path, sheet.GetName(), chunkRowStart, chunkRowEnd, full.Bytes()[start:end]))
		}
	}

	return fragments, full.Bytes()
}

func xlsRowValues(cols []structure.CellData) []string {
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		val := col.GetString()
		if val == "" {
			if num := col.GetFloat64(); num != 0 {
				val = strconv.FormatFloat(num, 'f', -1, 64)
			} else if in := col.GetInt64(); in != 0 {
				val = strconv.FormatInt(in, 10)
			}
		}
		out = append(out, val)
	}
	return out
}

func buildRowLineFromValues(rowIdx int, header []string, row []string) string {
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
		val := ""
		if col-1 < len(row) {
			val = row[col-1]
		}
		b.WriteString(val)
	}
	return b.String()
}

func newXLSFragment(start, end int, path, sheet string, rowStart, rowEnd int, data []byte) *document.Fragment {
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
		Kind:     "xls",
		Meta:     meta,
	}
}
