# grepFiles
Lexical search across files under a configured root.

Inputs (JSON):
- pattern: search pattern (supports "|" or "or" for OR)
- excludePattern: optional exclude pattern
- rootId: root id from roots
- path: path under root
- recursive: walk subtree
- include/exclude: glob patterns; supports **
- caseInsensitive: case-insensitive match
- mode: head|match (default match)
- lines/bytes: snippet limits
- maxFiles: cap matched files
- skipBinary/maxSize: filters

Output:
- stats: scanned/matched/truncated
- files: per-file snippets and match counts
