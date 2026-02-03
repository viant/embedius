Search for documents within a dataset root using vector similarity.

Inputs:
- root: dataset name (required)
- query: search query text (required)
- limit: max results (optional, default 10)
- min_score: minimum match score (optional)
- model: embedding model override (optional)

Outputs:
- results: list of matches with id, score, content, path, and metadata
