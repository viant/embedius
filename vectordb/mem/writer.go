//go:build sqlite
// +build sqlite

package mem

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/viant/embedius/vectordb/coord/sqlite"
)

type WriterOptions struct {
	Namespace      string
	BatchSize      int
	JournalTTL     time.Duration
	StaleReaderTTL time.Duration
	LeaseTTL       time.Duration
}

// Writer applies queued jobs to the namespace's ValueStore and emits events.
type Writer struct {
	store *Store
	opts  WriterOptions
}

func NewWriter(store *Store, opts WriterOptions) *Writer {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 64
	}
	if opts.JournalTTL <= 0 {
		opts.JournalTTL = time.Hour
	}
	if opts.StaleReaderTTL <= 0 {
		opts.StaleReaderTTL = 15 * time.Minute
	}
	return &Writer{store: store, opts: opts}
}

// WriterOpt mutates WriterOptions.
type WriterOpt func(*WriterOptions)

func WithWriterBatchOpt(n int) WriterOpt          { return func(o *WriterOptions) { o.BatchSize = n } }
func WithJournalTTLOpt(d time.Duration) WriterOpt { return func(o *WriterOptions) { o.JournalTTL = d } }
func WithStaleReaderTTLOpt(d time.Duration) WriterOpt {
	return func(o *WriterOptions) { o.StaleReaderTTL = d }
}

// RunNamespaceWriter is a convenience runner that constructs the writer with options and runs it.
func (s *Store) RunNamespaceWriter(ctx context.Context, namespace string, opts ...WriterOpt) error {
	wo := WriterOptions{Namespace: namespace}
	for _, opt := range opts {
		opt(&wo)
	}
	w := NewWriter(s, wo)
	return w.Run(ctx)
}

// Run processes jobs until ctx is done.
func (w *Writer) Run(ctx context.Context) error {
	ns := w.opts.Namespace
	coordPath := w.store.coordPathFor(ns)
	db, err := sqlite.Open(coordPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.EnsureSchema(ctx); err != nil {
		return err
	}

	// Open Set to get ValueStore and writer.lock
	set, err := NewSet(ctx, w.store.baseURL, ns, w.store.setOptions...)
	if err != nil {
		return err
	}
	defer set.close()

	owner := w.store.clientID() + "-" + time.Now().Format("20060102T150405")
	leaseTTL := w.opts.LeaseTTL
	// Acquire dynamic writer lease
	for {
		ok, err := db.TryAcquireWriter(ctx, owner, leaseTTL)
		if err != nil {
			return err
		}
		if ok {
			log.Printf("writer lease acquired: ns=%s owner=%s ttl=%s", ns, owner, leaseTTL)
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	// Heartbeat renewer
	hbTicker := time.NewTicker(leaseTTL / 3)
	defer hbTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = db.ReleaseWriter(context.Background(), owner)
			log.Printf("writer lease released (ctx done): ns=%s owner=%s", ns, owner)
			return ctx.Err()
		case <-hbTicker.C:
			if ok, _ := db.RenewWriter(ctx, owner); !ok {
				// Lost lease; release and exit to allow takeover
				_ = db.ReleaseWriter(context.Background(), owner)
				log.Printf("writer lease lost: ns=%s owner=%s", ns, owner)
				return fmt.Errorf("writer lease lost")
			}
		default:
		}
		jobs, err := db.ClaimPending(ctx, owner, w.opts.BatchSize)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				_ = db.ReleaseWriter(context.Background(), owner)
				log.Printf("writer lease released (idle ctx): ns=%s owner=%s", ns, owner)
				return ctx.Err()
			case <-ticker.C:
				continue
			}
		}
		// apply jobs
		var evs []sqlite.Event
		var doneIDs []int64
		var appendedIdx []int
		type jobResult struct {
			jobID       string
			addCount    int
			addStartPos int
		}
		var results []jobResult
		for _, j := range jobs {
			switch j.Kind {
			case "add":
				var payload addJobPayload
				if err := json.Unmarshal(j.Payload, &payload); err != nil {
					return fmt.Errorf("decode add job: %w", err)
				}
				jr := jobResult{jobID: j.JobID, addStartPos: len(evs)}
				for _, it := range payload.Items {
					// decode value
					val, err := base64.StdEncoding.DecodeString(it.ValueB64)
					if err != nil {
						return fmt.Errorf("decode value: %w", err)
					}
					// append to ValueStore
					ptr, err := set.values.Append(val)
					if err != nil {
						return fmt.Errorf("append: %w", err)
					}
					// update in-memory index for this process only
					set.mu.Lock()
					set.vectors = append(set.vectors, it.Vector)
					set.ptrs = append(set.ptrs, ptr)
					set.alive = append(set.alive, true)
					// append placeholder for ID; filled after InsertEvents assigns SCNs
					set.ids = append(set.ids, "")
					idx := len(set.vectors) - 1
					appendedIdx = append(appendedIdx, idx)
					set.mu.Unlock()
					// queue event
					evs = append(evs, sqlite.Event{Kind: "add", ID: "", Seg: ptr.SegmentID, Off: ptr.Offset, Len: ptr.Length, Ts: time.Now(), Vec: it.Vector})
				}
				jr.addCount = len(evs) - jr.addStartPos
				results = append(results, jr)
			case "remove":
				var payload removeJobPayload
				if err := json.Unmarshal(j.Payload, &payload); err != nil {
					return fmt.Errorf("decode remove job: %w", err)
				}
				for _, id := range payload.IDs {
					// attempt to tombstone if we can resolve ptr by id
					set.mu.RLock()
					idx, ok := set.id2idx[id]
					set.mu.RUnlock()
					if ok && idx >= 0 {
						set.mu.Lock()
						if idx < len(set.ptrs) && idx < len(set.alive) && set.alive[idx] {
							_ = set.values.Delete(set.ptrs[idx])
							set.alive[idx] = false
						}
						set.mu.Unlock()
					}
					evs = append(evs, sqlite.Event{Kind: "remove", ID: id, Ts: time.Now()})
				}
			default:
				// skip unknown job kind
			}
			doneIDs = append(doneIDs, j.ID)
		}
		start, err := db.InsertEvents(ctx, evs)
		if err != nil {
			return err
		}
		// Fill SCN-based IDs for appended items
		if len(appendedIdx) > 0 {
			set.mu.Lock()
			for i, idx := range appendedIdx {
				idStr := fmt.Sprintf("%d", start+uint64(i))
				if idx >= 0 && idx < len(set.ids) {
					set.ids[idx] = idStr
					if set.id2idx == nil {
						set.id2idx = map[string]int{}
					}
					set.id2idx[idStr] = idx
				}
			}
			set.mu.Unlock()
		}
		// Persist job results for add jobs
		if len(results) > 0 {
			for _, jr := range results {
				if jr.addCount <= 0 {
					continue
				}
				// SCNs for this job are contiguous starting at: start + jr.addStartPos
				scns := make([]string, jr.addCount)
				for i := 0; i < jr.addCount; i++ {
					scns[i] = fmt.Sprintf("%d", start+uint64(jr.addStartPos+i))
				}
				b, _ := json.Marshal(struct {
					SCNs []string `json:"scns"`
				}{SCNs: scns})
				_ = db.SetJobResult(ctx, jr.jobID, b)
			}
		}
		if err := db.MarkJobsDone(ctx, doneIDs); err != nil {
			return err
		}
		if w.store.writerMetrics != nil {
			adds, removes := 0, 0
			for _, e := range evs {
				if e.Kind == "add" {
					adds++
				} else if e.Kind == "remove" {
					removes++
				}
			}
			w.store.writerMetrics.OnBatchApplied(ns, len(jobs), adds, removes)
		}
		// optional snapshot persist each batch
		if err := set.persist(ctx); err != nil {
			return err
		}
	}
}
