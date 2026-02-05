# match
Semantic search over one or more roots.

Inputs (JSON):
- query: search query
- rootIds: optional list of root ids; defaults to all
- path: optional path prefix filter
- model: embedding model override
- maxDocuments: cap matched documents
- match: optional inclusion/exclusion settings
- limitBytes/cursor: pagination hints

Output:
- content: concatenated matched content (truncated by limitBytes)
- documents: matched documents with metadata
- documentsSize, cursor, limitBytes
- documentRoots map
