# Embedius

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/embedius)](https://goreportcard.com/report/github.com/viant/embedius)
[![GoDoc](https://godoc.org/github.com/viant/embedius?status.svg)](https://godoc.org/github.com/viant/embedius)

Embedius is a flexible vector database and document indexing solution for embedding local files and codebases into vector embeddings for semantic search, code understanding, and retrieval-augmented generation (RAG) applications.

## Features

- Lightweight vector database with persistent storage
- Context-aware document indexing
- Multiple indexers for different content types:
    - File system indexer for processing documents
    - Codebase indexer for intelligent code understanding
- Flexible content splitting strategies
- Integration with language model embedding providers
- Memory-efficient document storage
- In-memory vector search with cosine similarity

## Installation

```bash
go get github.com/viant/embedius
```

## Quick Start

### Basic Usage

```go
package main

import (
        "context"
        "fmt"
        "github.com/viant/embedius/embeddings"
        oai "github.com/viant/embedius/embeddings/openai"
        "github.com/viant/embedius/indexer"
        "github.com/viant/embedius/indexer/codebase"
        "github.com/viant/embedius/vectordb/mem"
        "path"
        "os"
)

func main() {
        // Initialize context
        ctx := context.Background()
        
        // Wire OpenAI embedder (or your own implementation)
        client := oai.NewClient(os.Getenv("OPENAI_API_KEY"), "text-embedding-3-small")
        var embedder embeddings.Embedder = &oai.Embedder{C: client}
        
        // Set storage location
        baseURL := path.Join(os.Getenv("HOME"), ".embedius")
        
        // Create in-memory vector database
        vectorDb := mem.NewStore(mem.WithBaseURL(baseURL))
        
        // Initialize codebase indexer
        codebaseIndexer := codebase.New("text-embedding-3-small")
        
        // Create indexing service
        service := indexer.NewService(
                baseURL,
                vectorDb,
                embedder,
                codebaseIndexer,
        )
        
        // Index a code repository
        projectPath := "/path/to/your/codebase"
        set, err := service.Add(ctx, projectPath)
        if err != nil {
                panic(err)
        }
        
        // Perform similarity search
        docs, err := set.SimilaritySearch(
                ctx,
                "how to handle authentication",
                5,
        )
        if err != nil {
                panic(err)
        }
        
        // Display results
        for i, doc := range docs {
                fmt.Printf("Result %d (score: %.4f):\n%s\n\n", 
                        i+1, doc.Score, doc.PageContent)
        }
}
```

## Architecture

Embedius consists of several key components:

### Vector Database

The vector database (`vectordb`) provides storage and retrieval of document embeddings:

- `mem.Store`: In-memory vector store with persistence support
- `vectordb.VectorStore`: Interface for vector storage operations
- `vectordb.Persister`: Interface for persistence operations

### Persistence (v2)

- Index file: `index_<namespace>.tr2` (atomic, durable)
  - Layout: `version(int16=2)` + `count(int32)` + repeated entries:
    - `alive(int16)` 0/1
    - `vector` length `int32` followed by `float32` elements
    - `Ptr` fields: `segmentId(int32)`, `offset(int64)`, `length(int32)`
  - Written atomically via temp file + rename; parent directory is fsynced.

- Values externalization:
  - Document payload bytes are stored in mmap-backed segments under `<baseURL>/data/<namespace>/` managed by `mmapstore`.
  - Files: `manifest.json`, `data_*.vdat`, optional `writer.lock` (not replicated).

- Zero-migration policy:
  - If legacy v1 files are detected (`index_<ns>.tre`/`index_<ns>.dat`), the loader wipes them and starts fresh.
  - Upstream sync also removes legacy `.tre/.dat` at destination to prevent confusion alongside `.tr2`.

- Single-writer guard:
  - A per-namespace `writer.lock` is held with an exclusive flock to prevent concurrent writers.
  - Configurable behavior: non-blocking, blocking with timeout, or indefinite wait (see options below).

### Queued Writes + Journal (SQLite)

- Overview: keep payload bytes in mmap `ValueStore`, coordinate multi‑process writes via a small SQLite DB per namespace.
  - Submitters enqueue jobs (`add`/`remove`) into `coord.db`.
  - A single writer process (holding `writer.lock`) claims jobs, appends values, emits journal `events` with SCNs, and persists `.tr2` snapshots.
  - Readers load `.tr2` and apply incremental events, updating a checkpoint.

- SCN as ID:
  - The writer assigns a strictly increasing SCN to each `add` event and stores it as the canonical document ID.
  - v3 `.tr2` snapshots persist IDs, so deletes by SCN remain valid even after journal GC.
  - Removes accept SCN strings as IDs; the writer emits a `remove` event with that ID, and readers mark the entry dead.

- Enqueue + Retrieve IDs (sqlite build):
  - Enqueue adds:
    - `jobID, _ := store.EnqueueAdds(ctx, docs, vectorstores.WithEmbedder(embedder), vectorstores.WithNameSpace(ns))`
  - Run writer for namespace:
    - `store.RunNamespaceWriter(ctx, ns, mem.WithWriterBatchOpt(64))`
  - Query results (SCN IDs):
    - `ids, status, _ := store.JobResult(ctx, ns, jobID)`
    - When `status == "done"`, `ids` contains SCN strings for the enqueued documents.

- Remove by SCN:
  - Submit a remove job with SCN IDs: `store.EnqueueRemoves(ctx, []string{"12345","12346"}, vectorstores.WithNameSpace(ns))`
  - The writer emits `remove` events; readers mark entries dead on next journal sync.

- Build/Run (writer/journal enabled):
  - Add the pure-Go driver: `go get modernc.org/sqlite`
  - Build with tag: `go build -tags sqlite ./...`
  - Optional CLI writer runner:
    - Build: `go build -tags sqlite ./cmd/embedius-writer`
    - Run: `./embedius-writer -base=/path/to/base -ns=default -batch=64 -lease=15s`

Example: enqueue → get SCN IDs → remove by SCN

```go
ctx := context.Background()
ns := "default"

// Create store with persistence and a reader tailer (optional)
store := mem.NewStore(
    mem.WithBaseURL(baseURL),
    mem.WithExternalValues(true),
    mem.WithTailInterval(500*time.Millisecond), // readers auto-sync
)

// 1) Enqueue add job (sqlite build required)
docs := []schema.Document{{PageContent: "Future of Technology"}, {PageContent: "Famous Fairytales"}}
jobID, err := store.EnqueueAdds(ctx, docs,
    vectorstores.WithEmbedder(embedder),
    vectorstores.WithNameSpace(ns),
)
if err != nil { panic(err) }

// 2) Run the writer loop for this namespace (in a separate goroutine/process).
// Enqueue opportunistically starts a short-lived writer too, but you can also
// run a sustained writer as shown below. Configure default lease TTL with
// mem.WithWriterLeaseTTL(...) on the Store.
go func() {
    if err := store.RunNamespaceWriter(ctx, ns, mem.WithWriterBatchOpt(64)); err != nil {
        panic(err)
    }
}()

// 3) Wait for job results to get SCN IDs (helper)
ids, status, err := store.WaitJobResult(ctx, ns, jobID, 250*time.Millisecond)
if err != nil { panic(err) }
if status != "done" || len(ids) == 0 { panic("job did not complete or no SCN IDs returned") }

// 4) Remove by SCN later
if _, err := store.EnqueueRemoves(ctx, []string{ids[0]}, vectorstores.WithNameSpace(ns)); err != nil {
    panic(err)
}
```

Notes:
- Readers opened with `WithReadOnly(true)` avoid taking `writer.lock` and can tail the journal via `WithTailInterval`.
- `.tr2` (v3) stores `snapshotSCN` and IDs; readers only replay events with `SCN > snapshotSCN`.

### Writer Lease & Tuning

- Lease TTL:
  - Configure default via `mem.WithWriterLeaseTTL(15 * time.Second)` on the Store.
  - Override default via `mem.WithWriterLeaseTTL(10 * time.Second)` on the Store.
  - Writer renews heartbeat periodically; if lost, it releases and exits so another process can take over.

- Logs/metrics:
  - Writer logs lease acquisition, release, and loss events (namespace, owner, TTL).
  - You can wrap `RunNamespaceWriter` in your own service for metrics export.


#### Configuring Persistence

- Enable persistence with external values and default segment size:

```go
vectorDb := mem.NewStore(
    mem.WithBaseURL(baseURL),             // enables persistence
    mem.WithExternalValues(true),         // use mmap-backed ValueStore by default
)
```

- Tune segment size and writer lock behavior:

```go
vectorDb := mem.NewStore(
    mem.WithBaseURL(baseURL),
    mem.WithExternalValues(true),
    mem.WithSegmentSize(512<<20),         // 512 MiB segments
    mem.WithWriterLock(true, 5*time.Second), // block up to 5s to acquire per-namespace lock
)
```

- Per-Set overrides:

```go
set, _ := mem.NewSet(ctx, baseURL, namespace,
    mem.WithSetExternalValues(true),
    mem.WithSetSegmentSize(256<<20), // 256 MiB
    mem.WithSetWriterLock(true, 0),  // block indefinitely
)
```

- Graceful shutdown:

```go
// Flushes .tr2 and segment manifests, closes mmaps and releases writer.lock
_ = vectorDb.Persist(ctx)
_ = vectorDb.(*mem.Store).Close()
```

Notes:
- Upstream synchronization copies `cache_<ns>.json`, `index_<ns>.tr2`, and files under `data/<ns>/` except `writer.lock`.
- `.tr2` writes use temp file + rename and fsync the parent dir to improve durability on supported filesystems.

### Indexer

The indexer processes content to create searchable documents:

- `indexer.Service`: Manages document sets for different locations
- `indexer.Set`: Collection of documents for a specific location
- `indexer.Indexer`: Interface for content indexing
- `indexer.Splitter`: Interface for splitting content into fragments

#### Codebase Indexer

The codebase indexer (`indexer/codebase`) is specialized for parsing and indexing code repositories:

- Intelligently analyzes code structure
- Preserves code semantics
- Extracts meaningful metadata

#### Filesystem Indexer

The file system indexer (`indexer/fs`) processes files from the filesystem:

- Supports various file types
- Implements content-aware splitting strategies
- Maintains document relationships

### Document Model

The document package defines the core data structures:

- `document.Entry`: Represents a complete document
- `document.Fragment`: Represents a portion of a document
- `document.Fragments`: Collection of document fragments

## Advanced Usage

### Customizing File System Indexing

```go
package main

import (
        "context"
        "github.com/viant/embedius/embeddings"
        "github.com/viant/embedius/indexer"
        "github.com/viant/embedius/indexer/fs"
        "github.com/viant/embedius/indexer/fs/matching"
        "github.com/viant/embedius/indexer/fs/splitter"
        "github.com/viant/embedius/vectordb/mem"
)

func main() {
        // Initialize components
        ctx := context.Background()
        baseURL := "/path/to/storage"
        embedder := getEmbedder() // Your embedder implementation
        
        // Create vector store
        vectorDb := mem.NewStore(mem.WithBaseURL(baseURL))
        
        // Configure matcher for file filtering
        matcher := matching.New(
                matching.WithExcludedExtensions(".jpg", ".png", ".gif"),
                matching.WithExcludedDirs("node_modules", ".git"),
        )
        
        // Configure content splitter factory
        splitterFactory := splitter.NewFactory(
                splitter.WithDefaultChunkSize(4096),
                splitter.WithLanguageDetection(true),
        )
        
        // Create filesystem indexer
        fsIndexer := fs.New(
                baseURL, 
                "text-embedding-3-small", 
                matcher, 
                splitterFactory,
        )
        
        // Create indexing service
        service := indexer.NewService(
                baseURL,
                vectorDb,
                embedder,
                fsIndexer,
        )
        
        // Index documents
        set, err := service.Add(ctx, "/path/to/documents")
        if err != nil {
                panic(err)
        }
        
        // Perform similarity search
        docs, err := set.SimilaritySearch(
                ctx,
                "example query",
                10,
        )
        // Process results...
}
```


## License


The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

## Credits

Developed and maintained by [Viant](https://github.com/viant).
