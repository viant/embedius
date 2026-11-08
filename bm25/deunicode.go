package bm25

import (
	_ "embed"
	"strings"
	"unicode/utf8"
)

//go:embed deunicode-data/pointers.bin
var transliterationPointers string

//go:embed deunicode-data/mapping.txt
var transliterationMapping string

func deunicodeChar(r rune) string {
	offset := int(r) * 3
	if offset+3 <= len(transliterationPointers) {
		size := int(transliterationPointers[offset+2])
		if size <= 2 {
			return transliterationPointers[offset : offset+size]
		}
		start := int(transliterationPointers[offset]) + int(transliterationPointers[offset+1])*256
		if start+size <= len(transliterationMapping) {
			return transliterationMapping[start : start+size]
		}
	}
	return "[?]"
}

func deunicode(text string) string {
	firstNonASCII := 0
	for firstNonASCII < len(text) && text[firstNonASCII] < 0x7f {
		firstNonASCII++
	}
	if firstNonASCII == len(text) {
		return text
	}
	var result strings.Builder
	result.Grow(len(text) | 15)
	result.WriteString(text[:firstNonASCII])
	text = text[firstNonASCII:]
	current, size := utf8.DecodeRuneInString(text)
	mapping := deunicodeChar(current)
	for text = text[size:]; len(text) != 0; text = text[size:] {
		next, nextSize := utf8.DecodeRuneInString(text)
		nextMapping := deunicodeChar(next)
		if len(mapping) > 1 && mapping[len(mapping)-1] == ' ' && strings.HasPrefix(nextMapping, " ") {
			mapping = mapping[:len(mapping)-1]
		}
		result.WriteString(mapping)
		mapping, size = nextMapping, nextSize
	}
	if len(mapping) > 1 && mapping[len(mapping)-1] == ' ' {
		mapping = mapping[:len(mapping)-1]
	}
	result.WriteString(mapping)
	return result.String()
}
