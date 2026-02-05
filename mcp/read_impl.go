package mcp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/mcp-protocol/extension"
)

type readTarget struct {
	fullURI  string
	normRoot string
	fullPath string
}

type readSelection struct {
	Text        string
	StartLine   int
	EndLine     int
	ModeApplied string
	Returned    int
	Remaining   int
	Binary      bool
	OffsetBytes int
}

func (h *Handler) read(ctx context.Context, in *ReadInput) (*ReadOutput, error) {
	if h == nil || h.service == nil {
		return nil, fmt.Errorf("mcp: service unavailable")
	}
	if in == nil {
		in = &ReadInput{}
	}
	target, err := h.resolveReadTarget(in)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(target.fullPath)
	if err != nil {
		return nil, err
	}
	selection, err := applyReadSelection(data, in)
	if err != nil {
		return nil, err
	}
	limitRequested := readLimitRequested(in)
	out := &ReadOutput{}
	populateReadOutput(out, target, selection, len(data), limitRequested)
	return out, nil
}

func (h *Handler) resolveReadTarget(in *ReadInput) (*readTarget, error) {
	if in.URI != "" {
		if strings.HasPrefix(in.URI, "file://") {
			path := strings.TrimPrefix(in.URI, "file://")
			if path == "" {
				return nil, fmt.Errorf("mcp: unsupported uri")
			}
			return &readTarget{fullURI: in.URI, normRoot: "", fullPath: path}, nil
		}
		return nil, fmt.Errorf("mcp: unsupported uri")
	}
	rootPath, err := h.resolveRootSpec(in.RootID, in.RootURI)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.Path) == "" {
		return nil, fmt.Errorf("mcp: missing path")
	}
	rootAbs, targetAbs, _, err := resolveRootTarget(rootPath, in.Path)
	if err != nil {
		return nil, err
	}
	return &readTarget{fullURI: fileURI(targetAbs), normRoot: rootAbs, fullPath: targetAbs}, nil
}

func readLimitRequested(input *ReadInput) bool {
	if input == nil {
		return false
	}
	if strings.TrimSpace(input.Mode) != "" {
		return true
	}
	if input.MaxBytes > 0 || input.LineCount > 0 {
		return true
	}
	if input.BytesRange.OffsetBytes > 0 || input.BytesRange.LengthBytes > 0 {
		return true
	}
	if input.StartLine > 0 {
		return true
	}
	return false
}

func applyReadSelection(data []byte, input *ReadInput) (*readSelection, error) {
	appliedMode := strings.TrimSpace(strings.ToLower(input.Mode))
	if appliedMode == "" {
		appliedMode = "head"
	}
	startLine := 0
	endLine := 0

	if isBinaryContent(data) {
		return &readSelection{
			Text:        "[binary content omitted]",
			StartLine:   0,
			EndLine:     0,
			ModeApplied: appliedMode,
			Returned:    0,
			Remaining:   len(data),
			Binary:      true,
			OffsetBytes: 0,
		}, nil
	}

	text := string(data)
	var returned, remaining, offsetBytes int
	if input.BytesRange.OffsetBytes > 0 || input.BytesRange.LengthBytes > 0 {
		clipped, start, _, err := clipBytesByRange(data, input.BytesRange)
		if err != nil {
			return nil, err
		}
		text = string(clipped)
		offsetBytes = start
		returned = len(text)
		remaining = len(data) - (start + returned)
		if remaining < 0 {
			remaining = 0
		}
	} else if input.StartLine > 0 {
		lineRange := LineRange{StartLine: input.StartLine, LineCount: input.LineCount}
		if lineRange.LineCount < 0 {
			lineRange.LineCount = 0
		}
		clipped, start, _, err := clipLinesByRange(data, lineRange)
		if err != nil {
			return nil, err
		}
		text = string(clipped)
		startLine = input.StartLine
		if lineRange.LineCount > 0 {
			endLine = startLine + lineRange.LineCount - 1
		}
		offsetBytes = start
		returned = len(text)
		remaining = len(data) - (start + returned)
		if remaining < 0 {
			remaining = 0
		}
	} else {
		maxBytes := input.MaxBytes
		if maxBytes <= 0 {
			maxBytes = defaultReadMaxBytes
		}
		maxLines := input.LineCount
		if maxLines < 0 {
			maxLines = 0
		}
		text, returned, remaining = applyMode(text, len(data), appliedMode, maxBytes, maxLines)
		offsetBytes = 0
	}
	return &readSelection{
		Text:        text,
		StartLine:   startLine,
		EndLine:     endLine,
		ModeApplied: appliedMode,
		Returned:    returned,
		Remaining:   remaining,
		Binary:      false,
		OffsetBytes: offsetBytes,
	}, nil
}

func applyMode(text string, totalSize int, mode string, maxBytes, maxLines int) (string, int, int) {
	switch mode {
	case "tail":
		return clipTail(text, totalSize, maxBytes, maxLines)
	case "signatures":
		if sig := extractSignatures(text, maxBytes); sig != "" {
			return sig, len(sig), clipRemaining(totalSize, len(sig))
		}
	}
	return clipHead(text, totalSize, maxBytes, maxLines)
}

func clipRemaining(totalSize, returned int) int {
	if totalSize <= returned {
		return 0
	}
	return totalSize - returned
}

func populateReadOutput(out *ReadOutput, target *readTarget, selection *readSelection, size int, limitRequested bool) {
	out.URI = target.fullURI
	if target.normRoot != "" {
		out.Path = relativePath(target.normRoot, target.fullPath)
	} else {
		out.Path = target.fullPath
	}
	out.Content = selection.Text
	out.Size = size
	out.Returned = selection.Returned
	out.Remaining = selection.Remaining
	out.StartLine = selection.StartLine
	out.EndLine = selection.EndLine
	out.ModeApplied = selection.ModeApplied
	out.Binary = selection.Binary
	truncated := selection.Returned > 0 && size > selection.Returned
	if !limitRequested {
		truncated = false
	}
	if selection.Remaining <= 0 && truncated {
		remaining := size - (selection.OffsetBytes + selection.Returned)
		if remaining < 0 {
			remaining = 0
		}
		out.Remaining = remaining
	}
	if limitRequested && (out.Remaining > 0 || truncated) {
		out.Continuation = &extension.Continuation{
			HasMore:   true,
			Remaining: out.Remaining,
			Returned:  out.Returned,
			Mode:      selection.ModeApplied,
			Binary:    selection.Binary,
		}
		if out.Continuation.Remaining < 0 {
			out.Continuation.Remaining = 0
		}
		if out.Continuation.Returned < 0 {
			out.Continuation.Returned = 0
		}
		nextOffset := selection.OffsetBytes + selection.Returned
		nextLength := selection.Returned
		if out.Remaining > 0 && nextLength > out.Remaining {
			nextLength = out.Remaining
		}
		if out.Remaining <= 0 {
			out.Continuation = nil
		} else {
			out.Continuation.NextRange = &extension.RangeHint{
				Bytes: &extension.ByteRange{Offset: nextOffset, Length: nextLength},
			}
			if out.EndLine > 0 && out.StartLine > 0 {
				count := out.EndLine - out.StartLine + 1
				if count < 0 {
					count = 0
				}
				out.Continuation.NextRange.Lines = &extension.LineRange{Start: out.EndLine + 1, Count: count}
			}
		}
	}
}

func relativePath(rootAbs string, abs string) string {
	rel, err := filepath.Rel(rootAbs, abs)
	if err != nil {
		return abs
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		return ""
	}
	return rel
}
