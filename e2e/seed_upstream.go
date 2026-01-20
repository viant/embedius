package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vector"
)

type logPayload struct {
	DatasetID      string `json:"dataset_id"`
	ID             string `json:"id"`
	Content        string `json:"content"`
	Meta           string `json:"meta"`
	Embedding      string `json:"embedding"`
	EmbeddingModel string `json:"embedding_model"`
	SCN            int64  `json:"scn"`
	Archived       int    `json:"archived"`
}

func main() {
	dbPath := flag.String("db", "", "sqlite db path")
	dataset := flag.String("dataset", "", "dataset id")
	id := flag.String("id", "", "document id")
	content := flag.String("content", "", "document content")
	path := flag.String("path", "", "document path")
	md5hex := flag.String("md5", "", "document md5")
	scn := flag.Int64("scn", 1, "scn value")
	archived := flag.Int("archived", 0, "archived flag")
	flag.Parse()

	if *dbPath == "" || *dataset == "" || *id == "" {
		fmt.Fprintln(os.Stderr, "db, dataset, and id are required")
		os.Exit(2)
	}

	db, err := engine.Open(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS vec_shadow_log (
		dataset_id   TEXT NOT NULL,
		shadow_table TEXT NOT NULL,
		scn          INTEGER NOT NULL,
		op           TEXT NOT NULL,
		document_id  TEXT NOT NULL,
		payload      BLOB NOT NULL,
		created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		fmt.Fprintf(os.Stderr, "create vec_shadow_log: %v\n", err)
		os.Exit(1)
	}

	vecs := embedString(*content, 64)
	blob, _ := vector.EncodeEmbedding(vecs)
	meta := map[string]interface{}{
		"asset_id": *id,
		"path":     *path,
		"md5":      *md5hex,
	}
	metaJSON, _ := json.Marshal(meta)
	p := logPayload{
		DatasetID:      *dataset,
		ID:             *id,
		Content:        *content,
		Meta:           string(metaJSON),
		Embedding:      hex.EncodeToString(blob),
		EmbeddingModel: "simple",
		SCN:            *scn,
		Archived:       *archived,
	}
	b, _ := json.Marshal(&p)

	if _, err := db.Exec(`INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload, created_at)
VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP)`, *dataset, "shadow_vec_docs", *scn, "insert", *id, b); err != nil {
		fmt.Fprintf(os.Stderr, "insert log: %v\n", err)
		os.Exit(1)
	}
}

func embedString(s string, dim int) []float32 {
	if dim <= 0 {
		dim = 64
	}
	v := make([]float32, dim)
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*16777619 ^ uint32(s[i])
	}
	seed := h
	for i := range v {
		seed = seed*1664525 + 1013904223
		v[i] = float32(seed%10000) / 10000.0
	}
	return v
}
