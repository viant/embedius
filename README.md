# Embedius

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/embedius)](https://goreportcard.com/report/github.com/viant/embedius)
[![GoDoc](https://godoc.org/github.com/viant/embedius?status.svg)](https://godoc.org/github.com/viant/embedius)

Embedius is a SQLite-backed vector indexing service for local files and upstream data, designed for semantic search and retrieval workflows.

## Features

- SQLite storage with `sqlite-vec` shadow tables
- Per-root SCN tracking and upstream sync
- File system indexing with include/exclude and size filters
- Config-driven multi-root indexing
- CLI and reusable service package

## Installation

```bash
go get github.com/viant/embedius
```

## SQLite Storage & CLI

Embedius stores vectors in SQLite via `sqlite-vec`. For the indexer package,
the default database file is `<baseURL>/embedius.sqlite`.

Schema DDLs:
- SQLite: `db/schema/sqlite/schema.ddl`
- MySQL upstream: `db/schema/mysql/schema.ddl`
- SCN triggers: `db/schema/sqlite/shadow_sync.ddl`, `db/schema/mysql/shadow_sync.ddl`

CLI (from `cmd/embedius`):

```bash
# Index a root (dataset) into SQLite
embedius index --root abc --path /abs/path/to/abc --db /tmp/vec.sqlite --progress

# Per-run include/exclude filters (comma-separated)
embedius index --root abc --path /abs/path/to/abc --db /tmp/vec.sqlite \\
  --include \"**/*.go,**/*.md\" --exclude \"**/vendor/**\" --max-size 10485760

# Index multiple roots from config
embedius index --config /path/to/roots.yaml --all --db /tmp/vec.sqlite

# Index all roots from config
embedius index --config /path/to/roots.yaml --all

# Force --db even when config has db:
embedius index --config /path/to/roots.yaml --all --db /tmp/vec.sqlite --db-force

# roots.yaml
# db: /tmp/vec.sqlite
# roots:
#   abc:
#     path: /abs/path/to/abc
#     include:
#       - "**/*.go"
#       - "**/*.md"
#     exclude:
#       - "**/node_modules/**"
#       - "**/.git/**"
#     max_size_bytes: 10485760
#   def:
#     path: /abs/path/to/def

# Query
embedius search --db /tmp/vec.sqlite --root abc --query "how does auth work?"

# Query using config
embedius search --config /path/to/roots.yaml --root abc --query "how does auth work?"

# Debug hold (for gops/pprof attach)
embedius search --db /tmp/vec.sqlite --root abc --query "auth" --embedder simple --debug-sleep 60

# Show root metadata
embedius roots --db /tmp/vec.sqlite
embedius roots --config /path/to/roots.yaml

# Upstream sync (optional)
embedius index --root abc --path /abs/path/to/abc --db /tmp/vec.sqlite \\
  --upstream-driver mysql --upstream-dsn 'user:pass@tcp(host:3306)/db' \\
  --upstream-shadow shadow_vec_docs --sync-batch 200

# Dedicated sync
embedius sync --root abc --db /tmp/vec.sqlite \\
  --upstream-driver mysql --upstream-dsn 'user:pass@tcp(host:3306)/db' \\
  --upstream-shadow shadow_vec_docs --sync-batch 200

# Sync multiple roots from config
embedius sync --config /path/to/roots.yaml --all --db /tmp/vec.sqlite \\
  --upstream-driver mysql --upstream-dsn 'user:pass@tcp(host:3306)/db'

# Sync with include/exclude filters
embedius sync --root abc --db /tmp/vec.sqlite \\
  --include "**/*.go" --exclude "**/vendor/**" --max-size 10485760 \\
  --upstream-driver mysql --upstream-dsn 'user:pass@tcp(host:3306)/db'

Note: sync filters apply only to insert/update; deletes are always applied.

# Force --db even when config has db:
embedius sync --config /path/to/roots.yaml --all --db /tmp/vec.sqlite --db-force \\
  --upstream-driver mysql --upstream-dsn 'user:pass@tcp(host:3306)/db'

# Admin tasks
embedius admin --db /tmp/vec.sqlite --root abc --action rebuild
embedius admin --db /tmp/vec.sqlite --root abc --action invalidate
embedius admin --db /tmp/vec.sqlite --root abc --action prune
embedius admin --db /tmp/vec.sqlite --root abc --action prune --scn 12345 --force
embedius admin --db /tmp/vec.sqlite --root abc --action check

# Admin across all roots from config
embedius admin --config /path/to/roots.yaml --all --action rebuild --db /tmp/vec.sqlite

# Build with upstream drivers
go build -tags mysql ./cmd/embedius
go build -tags postgres ./cmd/embedius
```

## Endly E2E

Endly E2E entry point is `e2e/run.yaml` (runs sequentially):

```bash
cd e2e
endly run.yaml
```

## Concepts

- Root (dataset): named logical source (e.g., `abc`) mapped to a local path.
- Assets: files under a root, tracked with `md5`, size, and `scn`.
- Documents: chunks derived from assets; stored in `_vec_emb_docs`.
- SCN: per-root sequence for change tracking and upstream sync.
- Shadow tables: `_vec_*` tables used by `sqlite-vec` for vector search.

## Service Package (Library Use)

Use `embedius/service` when you want to embed indexing/search/sync/admin flows
directly in your program without invoking the CLI.

```go
package main

import (
	"context"
	"log"

	"github.com/viant/embedius/embeddings/openai"
	"github.com/viant/embedius/service"
)

func main() {
	ctx := context.Background()
	embedder := &openai.Embedder{C: openai.NewClient("KEY", "text-embedding-3-small")}

	svc, err := service.NewService(
		service.WithDSN("/tmp/embedius.sqlite"),
		service.WithEmbedder(embedder),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close()

	roots, _, err := service.ResolveRoots(service.ResolveRootsRequest{
		Root:        "abc",
		RootPath:    "/abs/path/to/abc",
		RequirePath: true,
		Include:     []string{"**/*.go"},
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := svc.Index(ctx, service.IndexRequest{
		Roots:   roots,
		Model:   "text-embedding-3-small",
		Logf:    log.Printf,
		Prune:   true,
		BatchSize: 64,
	}); err != nil {
		log.Fatal(err)
	}

	results, err := svc.Search(ctx, service.SearchRequest{
		Dataset: "abc",
		Query:   "how does auth work?",
		Limit:   5,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("matches=%d", len(results))
}
```

## License


The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

## Credits

Developed and maintained by [Viant](https://github.com/viant).
