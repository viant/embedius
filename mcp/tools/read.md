# read
Read a file under a configured root.

Inputs (JSON):
- rootId: root id from roots (required unless uri is provided)
- path: file path under root (required unless uri is provided)
- uri: optional file:// uri
- bytesRange: {offsetBytes,lengthBytes}
- startLine/lineCount: optional line range
- maxBytes: cap returned content
- mode: head|tail|signatures (default head)

Output:
- uri, path, content, size
- returned/remaining
- startLine/endLine
- binary, modeApplied
- continuation: next range hint when truncated
