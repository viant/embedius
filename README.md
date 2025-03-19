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
        "github.com/tmc/langchaingo/embeddings"
        "github.com/tmc/langchaingo/llms/openai"
        "github.com/viant/embedius/indexer"
        "github.com/viant/embedius/indexer/codebase"
        "github.com/viant/embedius/vectordb/mem"
        "path"
        "os"
)

func main() {
        // Initialize context
        ctx := context.Background()
        
        // Set up your embedding model
        model, err := openai.New(
                openai.WithToken(os.Getenv("OPENAI_API_KEY")),
                openai.WithEmbeddingModel("text-embedding-3-small"),
        )
        if err != nil {
                panic(err)
        }
        
        // Create embedder
        embedder, err := embeddings.NewEmbedder(model)
        if err != nil {
                panic(err)
        }
        
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
        "github.com/tmc/langchaingo/embeddings"
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

