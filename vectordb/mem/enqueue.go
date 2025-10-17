package mem

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/viant/bintly"
	"github.com/viant/embedius/schema"
	vdb "github.com/viant/embedius/vectordb"
	"github.com/viant/embedius/vectordb/coord/sqlite"
	"github.com/viant/embedius/vectorstores"
)

// addJobPayload is the JSON envelope stored in jobs.payload for kind='add'.
type addJobPayload struct {
	Items []addJobItem `json:"items"`
}
type addJobItem struct {
	ID       string    `json:"id"`
	Vector   []float32 `json:"vector"`
	ValueB64 string    `json:"value_b64"`
}

// removeJobPayload is the JSON envelope for kind='remove'.
type removeJobPayload struct {
	IDs []string `json:"ids"`
}

// EnqueueAdds embeds and encodes documents, then enqueues a job in the coordination DB.
// Requires WithJobQueue(true) and a valid baseURL. Returns generated jobID.
func (s *Store) EnqueueAdds(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) (string, error) {
	if len(docs) == 0 {
		return "", nil
	}
	ns := s.getSetName(opts)
	if s.baseURL == "" {
		return "", fmt.Errorf("EnqueueAdds: baseURL required")
	}
	// Embed documents
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.Embedder == nil {
		return "", fmt.Errorf("EnqueueAdds: embedder is required")
	}
	texts := make([]string, len(docs))
	for i := range docs {
		texts[i] = docs[i].PageContent
	}
	vectors, err := options.Embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return "", err
	}
	if len(vectors) != len(docs) {
		return "", fmt.Errorf("embedder returned %d vectors, expected %d", len(vectors), len(docs))
	}

	// Encode documents
	writers := bintly.NewWriters()
	payload := addJobPayload{Items: make([]addJobItem, len(docs))}
	for i := range docs {
		var dv vdb.Document = vdb.Document(docs[i])
		w := writers.Get()
		if err := dv.EncodeBinary(w); err != nil {
			writers.Put(w)
			return "", err
		}
		bs := w.Bytes()
		writers.Put(w)
		payload.Items[i] = addJobItem{
			ID:       fmt.Sprintf("%s:%d", ns, time.Now().UnixNano()+int64(i)),
			Vector:   vectors[i],
			ValueB64: base64.StdEncoding.EncodeToString(bs),
		}
	}
	body, err := json.Marshal(&payload)
	if err != nil {
		return "", err
	}

	// Open coord DB
	coordPath := s.coordPathFor(ns)
	if err := os.MkdirAll(filepath.Dir(coordPath), 0o755); err != nil {
		return "", err
	}
	db, err := sqlite.Open(coordPath)
	if err != nil {
		return "", err
	}
	defer db.Close()
	if err := db.EnsureSchema(ctx); err != nil {
		return "", err
	}

	clientID := s.clientID()
	jobID := uuid.NewString()
	if _, err := db.EnqueueJob(ctx, "add", body, clientID, jobID); err != nil {
		return "", err
	}
	// Opportunistically start a short-lived writer to process queue until idle.
	go s.RunNamespaceWriterUntilIdle(context.Background(), ns, 2*time.Second)
	return jobID, nil
}

// EnqueueRemoves enqueues a remove job listing ids to delete.
func (s *Store) EnqueueRemoves(ctx context.Context, ids []string, opts ...vectorstores.Option) (string, error) {
	if len(ids) == 0 {
		return "", nil
	}
	ns := s.getSetName(opts)
	if s.baseURL == "" {
		return "", fmt.Errorf("EnqueueRemoves: baseURL required")
	}
	body, err := json.Marshal(&removeJobPayload{IDs: ids})
	if err != nil {
		return "", err
	}
	coordPath := s.coordPathFor(ns)
	if err := os.MkdirAll(filepath.Dir(coordPath), 0o755); err != nil {
		return "", err
	}
	db, err := sqlite.Open(coordPath)
	if err != nil {
		return "", err
	}
	defer db.Close()
	if err := db.EnsureSchema(ctx); err != nil {
		return "", err
	}
	clientID := s.clientID()
	jobID := uuid.NewString()
	if _, err := db.EnqueueJob(ctx, "remove", body, clientID, jobID); err != nil {
		return "", err
	}
	go s.RunNamespaceWriterUntilIdle(context.Background(), ns, 2*time.Second)
	return jobID, nil
}

func (s *Store) coordPathFor(ns string) string {
	// If any Set override exists, we could honor it later; default path:
	if s.baseURL == "" {
		return "coord.db"
	}
	return filepath.Join(s.baseURL, "data", ns, "coord.db")
}

func (s *Store) clientID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s@%d", host, os.Getpid())
}

// JobResult returns the result for a previously enqueued job (SCN IDs for add jobs),
// and the current job status.
func (s *Store) JobResult(ctx context.Context, namespace, jobID string) (scnIDs []string, status string, err error) {
	if s.baseURL == "" {
		return nil, "", fmt.Errorf("JobResult: baseURL required")
	}
	coordPath := s.coordPathFor(namespace)
	db, err := sqlite.Open(coordPath)
	if err != nil {
		return nil, "", err
	}
	defer db.Close()
	if err := db.EnsureSchema(ctx); err != nil {
		return nil, "", err
	}
	res, status, err := db.GetJobResult(ctx, jobID)
	if err != nil {
		return nil, "", err
	}
	if len(res) == 0 {
		return nil, status, nil
	}
	var parsed struct {
		SCNs []string `json:"scns"`
	}
	if err := json.Unmarshal(res, &parsed); err != nil {
		return nil, status, err
	}
	return parsed.SCNs, status, nil
}

// WaitJobResult polls the coordination DB for a job result until ctx is done or the job reaches status 'done'.
// For add jobs, it returns SCN IDs. For remove jobs, the returned SCN list is empty and status will be 'done'.
// If interval <= 0, a default of 200ms is used.
func (s *Store) WaitJobResult(ctx context.Context, namespace, jobID string, interval time.Duration) (scnIDs []string, status string, err error) {
	if s.baseURL == "" {
		return nil, "", fmt.Errorf("WaitJobResult: baseURL required")
	}
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	coordPath := s.coordPathFor(namespace)
	db, err := sqlite.Open(coordPath)
	if err != nil {
		return nil, "", err
	}
	defer db.Close()
	if err := db.EnsureSchema(ctx); err != nil {
		return nil, "", err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		default:
		}
		res, st, err := db.GetJobResult(ctx, jobID)
		if err != nil {
			return nil, "", err
		}
		if st == "done" {
			if len(res) == 0 {
				return nil, st, nil
			}
			var parsed struct {
				SCNs []string `json:"scns"`
			}
			if err := json.Unmarshal(res, &parsed); err != nil {
				return nil, st, err
			}
			return parsed.SCNs, st, nil
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-ticker.C:
		}
	}
}
