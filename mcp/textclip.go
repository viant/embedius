package mcp

import (
	"errors"
	"regexp"
	"strings"
)

type intRange struct {
	from *int
	to   *int
}

func clipBytesByRange(b []byte, br BytesRange) ([]byte, int, int, error) {
	if br.OffsetBytes <= 0 && br.LengthBytes <= 0 {
		return b, 0, len(b), nil
	}
	start := int(br.OffsetBytes)
	end := start
	if br.LengthBytes > 0 {
		end = start + br.LengthBytes
	} else {
		end = len(b)
	}
	r := &intRange{from: &start, to: &end}
	return clipBytes(b, r)
}

func clipLinesByRange(b []byte, lr LineRange) ([]byte, int, int, error) {
	if lr.StartLine <= 0 && lr.LineCount <= 0 {
		return b, 0, len(b), nil
	}
	from := lr.StartLine - 1
	if from < 0 {
		from = 0
	}
	to := from
	if lr.LineCount > 0 {
		to = from + lr.LineCount
	} else {
		to = from + 1_000_000_000
	}
	r := &intRange{from: &from, to: &to}
	return clipLines(b, r)
}

func clipBytes(b []byte, r *intRange) ([]byte, int, int, error) {
	if r == nil || r.from == nil || r.to == nil || *r.from < 0 || *r.to < *r.from {
		return nil, 0, 0, errors.New("invalid byteRange")
	}
	start := *r.from
	end := *r.to
	if start < 0 {
		start = 0
	}
	if start > len(b) {
		start = len(b)
	}
	if end < start {
		end = start
	}
	if end > len(b) {
		end = len(b)
	}
	return b[start:end], start, end, nil
}

func clipLines(b []byte, r *intRange) ([]byte, int, int, error) {
	if r == nil || r.from == nil || r.to == nil || *r.from < 0 || *r.to < *r.from {
		return nil, 0, 0, errors.New("invalid lineRange")
	}
	starts := []int{0}
	for i, c := range b {
		if c == '\n' && i+1 < len(b) {
			starts = append(starts, i+1)
		}
	}
	total := len(starts)
	from := *r.from
	to := *r.to
	if from < 0 {
		from = 0
	}
	if from > total {
		from = total
	}
	if to < from {
		to = from
	}
	if to > total {
		to = total
	}
	start := 0
	if from < total {
		start = starts[from]
	} else {
		start = len(b)
	}
	end := len(b)
	if to-1 < total-1 {
		end = starts[to] - 1
	}
	if end < start {
		end = start
	}
	return b[start:end], start, end, nil
}

func clipHead(text string, totalSize, maxBytes, maxLines int) (string, int, int) {
	if maxLines <= 0 {
		head := text
		if maxBytes > 0 && len(head) > maxBytes {
			head = head[:maxBytes]
		}
		return head, len(head), remaining(totalSize, len(head))
	}
	lines := strings.Split(text, "\n")
	if len(lines) > maxLines {
		lines = lines[:maxLines]
	}
	head := strings.Join(lines, "\n")
	return head, len(head), remaining(totalSize, len(head))
}

func clipTail(text string, totalSize, maxBytes, maxLines int) (string, int, int) {
	if maxLines <= 0 {
		tail := text
		if maxBytes > 0 && len(tail) > maxBytes {
			tail = tail[len(tail)-maxBytes:]
		}
		return tail, len(tail), remaining(totalSize, len(tail))
	}
	lines := strings.Split(text, "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	tail := strings.Join(lines, "\n")
	return tail, len(tail), remaining(totalSize, len(tail))
}

func extractSignatures(text string, maxBytes int) string {
	var sigs []string
	re := regexp.MustCompile(`^\\s*(public|private|protected|class|interface|func|def|package|import|\\w+\\s+\\w+\\()`)
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if re.MatchString(line) {
			sigs = append(sigs, line)
		}
		if maxBytes > 0 && len(strings.Join(sigs, "\n")) >= maxBytes {
			break
		}
	}
	result := strings.Join(sigs, "\n")
	if maxBytes > 0 && len(result) > maxBytes {
		result = result[:maxBytes]
	}
	return result
}

func remaining(totalSize, returned int) int {
	if totalSize <= returned {
		return 0
	}
	return totalSize - returned
}
