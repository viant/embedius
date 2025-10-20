package mem

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/viant/embedius/schema"
	"github.com/viant/embedius/vectorstores"

	"github.com/viant/bintly"
	vdb "github.com/viant/embedius/vectordb"
	coord "github.com/viant/embedius/vectordb/coord/sqlite"
	vstorage "github.com/viant/embedius/vectordb/storage"
	"github.com/viant/embedius/vectordb/storage/memstore"
	"github.com/viant/embedius/vectordb/storage/mmapstore"
	"github.com/viant/gds/tree/cover"
)

// Set is an in-memory vector set that stores vectors in RAM and keeps
// only pointers (to externally stored document payloads) in memory for values.
type Set struct {
	name    string
	baseURL string

	// external value storage (can be mmap-backed or in-memory)
	values vstorage.ValueStore
	// feature flags and tunables
	externalValues bool
	segmentSize    int64

	// single-writer file lock
	lockFile *os.File
	// lock behavior
	lockBlocking bool
	lockTimeout  time.Duration

	// writer queue gate (multi-process fairness)
	writerQueue    bool
	writerGatePoll time.Duration
	writerGateTTL  time.Duration

	// coordination (Phase 0 prep)
	jobQueue  bool   // enable SQLite-backed job queue
	coordPath string // custom coordination DB path (optional)

	// configuration (Phase 5)
	readOnly       bool
	journalTTL     time.Duration
	staleReaderTTL time.Duration
	writerBatch    int

	// in-memory index data
	mu      sync.RWMutex
	vectors [][]float32
	ptrs    []vstorage.Ptr
	alive   []bool // logical liveness for removals
	ids     []string

	// journal snapshot coordination
	snapshotSCN uint64
	id2idx      map[string]int

	// cover tree index for kNN
	coverBase float32
	coverDist cover.DistanceFunction
	tree      *cover.Tree[int]

	// tailer
	tailInterval time.Duration
	tailCancel   context.CancelFunc
	tailWG       sync.WaitGroup
}

// NewSet creates a new in-memory Set. If no ValueStore was injected via options,
// it defaults to an in-memory ValueStore for Phase 2 wiring.
func NewSet(ctx context.Context, baseURL string, name string, opts ...SetOption) (*Set, error) {
	s := &Set{
		name:      name,
		baseURL:   baseURL,
		coverBase: 1.3,
		coverDist: cover.DistanceFunctionCosine,
	}
	// default: enable external values if baseURL provided
	if baseURL != "" {
		s.externalValues = true
	}
	for _, opt := range opts {
		opt(s)
	}
	// Default value store: if baseURL is provided, use mmapstore in
	// baseURL/data/<namespace>; otherwise fall back to in-memory store.
	if s.values == nil {
		if s.externalValues && baseURL != "" {
			dataDir := filepath.Join(baseURL, "data", name)
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				return nil, err
			}
			if !s.readOnly {
				// Optional queued writer gate
				if s.writerQueue {
					if s.writerGatePoll <= 0 {
						s.writerGatePoll = 100 * time.Millisecond
					}
					if s.writerGateTTL <= 0 {
						s.writerGateTTL = 60 * time.Second
					}
					if err := s.enterWriterGate(ctx, dataDir); err != nil {
						return nil, err
					}
				}
				// Acquire per-namespace writer lock
				lockPath := filepath.Join(dataDir, "writer.lock")
				lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
				if err != nil {
					return nil, fmt.Errorf("open lock: %w", err)
				}
				if s.lockBlocking {
					if s.lockTimeout <= 0 {
						if err := lockExclusiveBlocking(lf); err != nil {
							_ = lf.Close()
							return nil, fmt.Errorf("namespace '%s' lock failed: %v", name, err)
						}
					} else {
						deadline := time.Now().Add(s.lockTimeout)
						for {
							err := tryLockExclusive(lf)
							if err == nil {
								break
							}
							if errors.Is(err, errWouldBlock) {
								if time.Now().After(deadline) {
									_ = lf.Close()
									return nil, fmt.Errorf("%w: %s (after %s)", ErrNamespaceLockTimeout, name, s.lockTimeout)
								}
								time.Sleep(50 * time.Millisecond)
								continue
							}
							_ = lf.Close()
							return nil, fmt.Errorf("namespace '%s' lock failed: %v", name, err)
						}
					}
				} else {
					err := tryLockExclusive(lf)
					if errors.Is(err, errWouldBlock) {
						_ = lf.Close()
						return nil, fmt.Errorf("%w: %s", ErrNamespaceLocked, name)
					}
					if err != nil {
						_ = lf.Close()
						return nil, fmt.Errorf("namespace '%s' lock failed: %v", name, err)
					}
				}
				s.lockFile = lf
			}

			opts := mmapstore.Options{BasePath: dataDir}
			if s.segmentSize > 0 {
				opts.SegmentSize = s.segmentSize
			}
			vs, err := mmapstore.Open(opts)
			if err != nil {
				return nil, err
			}
			s.values = vs
		} else {
			s.values = memstore.New()
		}
	}
	// Attempt to load existing index (v2/v3) or migrate from v1.
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	// Apply any journaled changes (best-effort)
	s.syncFromJournal(ctx)

	// Initialize cover tree and (re)build from in-memory vectors
	if s.coverBase <= 0 {
		s.coverBase = 1.3
	}
	if s.coverDist == "" {
		s.coverDist = cover.DistanceFunctionCosine
	}
	s.tree = cover.NewTree[int](s.coverBase, s.coverDist)
	// Use level-bound strategy to avoid per-node radius writes during concurrent queries
	s.tree.SetBoundStrategy(cover.BoundLevel)
	s.rebuildCoverIndex()
	// Start background tailer if configured
	if s.tailInterval > 0 {
		s.startTailer()
	}
	return s, nil
}

// rebuildCoverIndex rebuilds the cover tree from all alive vectors.
func (s *Set) rebuildCoverIndex() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.tree == nil {
		return
	}
	// Recreate the tree to avoid stale entries
	base := s.coverBase
	dist := s.coverDist
	s.tree = cover.NewTree[int](base, dist)
	s.tree.SetBoundStrategy(cover.BoundLevel)
	for i := range s.vectors {
		if i >= len(s.alive) || !s.alive[i] {
			continue
		}
		vec := s.vectors[i]
		if len(vec) == 0 {
			continue
		}
		pt := cover.NewPoint(vec...)
		_ = s.tree.Insert(i, pt)
	}
}

// persist writes the set state in v3 layout: snapshotSCN + vectors + Ptrs + IDs.
func (s *Set) persist(ctx context.Context) error {
	if s.baseURL == "" {
		return nil
	}
	s.mu.RLock()
	vectors := s.vectors
	ptrs := s.ptrs
	alive := s.alive
	ids := s.ids
	s.mu.RUnlock()

	path := filepath.Join(s.baseURL, fmt.Sprintf("index_%s.tr2", s.name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	writers := bintly.NewWriters()
	w := writers.Get()
	// Determine snapshot SCN (best-effort)
	snapshotSCN := int64(s.snapshotSCN)
	// Try to get last_scn from coord DB if available
	coordPath := filepath.Join(s.baseURL, "data", s.name, "coord.db")
	if db, err := coord.Open(coordPath); err == nil {
		if err := db.EnsureSchema(ctx); err == nil {
			if last, err := db.GetLastSCN(ctx); err == nil {
				snapshotSCN = int64(last)
			}
		}
		_ = db.Close()
	}
	s.snapshotSCN = uint64(snapshotSCN)
	// layoutVersion=3
	w.Int16(3)
	// snapshotSCN
	w.Int64(snapshotSCN)
	// count
	w.Int32(int32(len(vectors)))
	for i := range vectors {
		// alive flag as Int16 0/1
		if i < len(alive) && alive[i] {
			w.Int16(1)
		} else {
			w.Int16(0)
		}
		// vector
		v := vectors[i]
		w.Int32(int32(len(v)))
		for j := range v {
			w.Float32(v[j])
		}
		// ptr: segID(int32), offset(int64), length(int32)
		var p vstorage.Ptr
		if i < len(ptrs) {
			p = ptrs[i]
		}
		w.Int32(int32(p.SegmentID))
		w.Int64(int64(p.Offset))
		w.Int32(int32(p.Length))
		// id string (may be empty if not available)
		id := ""
		if i < len(ids) {
			id = ids[i]
		}
		w.String(id)
	}
	if _, err := f.Write(w.Bytes()); err != nil {
		writers.Put(w)
		return err
	}
	writers.Put(w)
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// fsync parent directory to persist the rename on supported filesystems
	if dirf, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dirf.Sync()
		_ = dirf.Close()
	}
	return nil
}

// load loads v2 index if present; if v1 is detected, migrates to v2.
func (s *Set) load(_ context.Context) error {
	if s.baseURL == "" {
		return nil
	}
	path := filepath.Join(s.baseURL, fmt.Sprintf("index_%s.tr2", s.name))
	data, err := os.ReadFile(path)
	if err != nil {
		// If new index not found, detect old files and wipe them
		if errors.Is(err, os.ErrNotExist) {
			oldTre := filepath.Join(s.baseURL, fmt.Sprintf("index_%s.tre", s.name))
			if _, statErr := os.Stat(oldTre); statErr == nil {
				_ = s.wipeOldStore()
			}
			return nil
		}
		return err
	}
	readers := bintly.NewReaders()
	r := readers.Get()
	_ = r.FromBytes(data)
	var version int16
	r.Int16(&version)
	switch version {
	case 3:
		// v3: snapshotSCN + entries with vectors, ptrs, ids
		var snap int64
		r.Int64(&snap)
		s.snapshotSCN = uint64(snap)
		var n int32
		r.Int32(&n)
		if n < 0 {
			readers.Put(r)
			return fmt.Errorf("%w: invalid count %d", ErrIndexCorrupt, n)
		}
		vectors := make([][]float32, int(n))
		ptrs := make([]vstorage.Ptr, int(n))
		alive := make([]bool, int(n))
		ids := make([]string, int(n))
		var dim0 int32 = -1
		for i := 0; i < int(n); i++ {
			var aliveInt int16
			r.Int16(&aliveInt)
			alive[i] = aliveInt != 0
			var m int32
			r.Int32(&m)
			if m < 0 {
				readers.Put(r)
				return fmt.Errorf("%w: invalid vector length at %d: %d", ErrIndexCorrupt, i, m)
			}
			if dim0 == -1 {
				dim0 = m
			} else if m != dim0 {
				readers.Put(r)
				return fmt.Errorf("%w: inconsistent vector dimension at %d: %d vs %d", ErrIndexCorrupt, i, m, dim0)
			}
			vec := make([]float32, int(m))
			for j := 0; j < int(m); j++ {
				r.Float32(&vec[j])
			}
			vectors[i] = vec
			var segID int32
			var off int64
			var length int32
			r.Int32(&segID)
			r.Int64(&off)
			r.Int32(&length)
			ptrs[i] = vstorage.Ptr{SegmentID: uint32(segID), Offset: uint64(off), Length: uint32(length)}
			var id string
			r.String(&id)
			ids[i] = id
		}
		s.mu.Lock()
		s.vectors, s.ptrs, s.alive, s.ids = vectors, ptrs, alive, ids
		if s.id2idx == nil {
			s.id2idx = map[string]int{}
		}
		for i := range ids {
			if ids[i] != "" && alive[i] {
				s.id2idx[ids[i]] = i
			}
		}
		s.mu.Unlock()
		readers.Put(r)
		return nil
	case 2:
		var n int32
		r.Int32(&n)
		if n < 0 {
			return fmt.Errorf("%w: invalid count %d", ErrIndexCorrupt, n)
		}
		vectors := make([][]float32, int(n))
		ptrs := make([]vstorage.Ptr, int(n))
		alive := make([]bool, int(n))
		var dim0 int32 = -1
		// expected size in bytes: version(2) + count(4)
		required := int64(2 + 4)
		for i := 0; i < int(n); i++ {
			var aliveInt int16
			r.Int16(&aliveInt)
			alive[i] = aliveInt != 0
			var m int32
			r.Int32(&m)
			if m < 0 {
				return fmt.Errorf("%w: invalid vector length at %d: %d", ErrIndexCorrupt, i, m)
			}
			if dim0 == -1 {
				dim0 = m
			} else if m != dim0 {
				return fmt.Errorf("%w: inconsistent vector dimension at %d: got %d, want %d", ErrIndexCorrupt, i, m, dim0)
			}
			// check that vector bytes do not overflow required size calculation
			vecBytes := int64(m) * 4
			if vecBytes < 0 {
				return fmt.Errorf("%w: overflow computing vector bytes at %d", ErrIndexCorrupt, i)
			}
			vec := make([]float32, int(m))
			for j := 0; j < int(m); j++ {
				r.Float32(&vec[j])
			}
			vectors[i] = vec
			var segID int32
			var off int64
			var length int32
			r.Int32(&segID)
			r.Int64(&off)
			r.Int32(&length)
			if off < 0 || length < 0 {
				return fmt.Errorf("%w: invalid pointer at %d (off=%d,len=%d)", ErrIndexCorrupt, i, off, length)
			}
			ptrs[i] = vstorage.Ptr{SegmentID: uint32(segID), Offset: uint64(off), Length: uint32(length)}
			// accumulate required bytes for this entry: alive(2)+len(4)+vec(4*m)+segID(4)+off(8)+len(4)
			required += int64(2+4) + vecBytes + int64(4+8+4)
			if required > int64(len(data)) {
				return fmt.Errorf("%w: index truncated while reading entry %d (need %d, have %d)", ErrIndexCorrupt, i, required, len(data))
			}
		}
		// Allow extra trailing bytes for forward-compatible extensions; we only require no truncation.
		s.mu.Lock()
		s.vectors, s.ptrs, s.alive = vectors, ptrs, alive
		s.mu.Unlock()
		readers.Put(r)
		return nil
	case 1:
		// Do not migrate; wipe old store and start from scratch
		readers.Put(r)
		_ = s.wipeOldStore()
		return nil
	default:
		readers.Put(r)
		return fmt.Errorf("unsupported index version: %d", version)
	}
}

// syncFromJournal applies events since snapshot to update the in-memory index.
// Best-effort; ignored if coordination DB or sqlite tag is not available.
func (s *Set) syncFromJournal(ctx context.Context) {
	if s.baseURL == "" || s.name == "" {
		return
	}
	coordPath := filepath.Join(s.baseURL, "data", s.name, "coord.db")
	db, err := coord.Open(coordPath)
	if err != nil {
		return
	}
	defer db.Close()
	if err := db.EnsureSchema(ctx); err != nil {
		return
	}
	// For now, treat snapshot_scn as 0 (future: store in .tr2)
	rows, err := db.EventsSince(ctx, s.snapshotSCN)
	if err != nil {
		return
	}
	var applied uint64
	for _, r := range rows {
		applied = r.SCN
		switch r.Ev.Kind {
		case "add":
			s.mu.Lock()
			s.vectors = append(s.vectors, r.Ev.Vec)
			s.ptrs = append(s.ptrs, vstorage.Ptr{SegmentID: r.Ev.Seg, Offset: r.Ev.Off, Length: r.Ev.Len})
			s.alive = append(s.alive, true)
			s.ids = append(s.ids, r.Ev.ID)
			if s.id2idx == nil {
				s.id2idx = map[string]int{}
			}
			s.id2idx[r.Ev.ID] = len(s.vectors) - 1
			// update cover index if enabled
			if s.tree != nil && len(r.Ev.Vec) > 0 {
				pt := cover.NewPoint(r.Ev.Vec...)
				_ = s.tree.Insert(len(s.vectors)-1, pt)
			}
			s.mu.Unlock()
		case "remove":
			s.mu.Lock()
			if s.id2idx != nil {
				if idx, ok := s.id2idx[r.Ev.ID]; ok && idx >= 0 && idx < len(s.alive) {
					s.alive[idx] = false
				}
			}
			s.mu.Unlock()
		}
	}
	// Update checkpoint (best-effort)
	if applied > 0 {
		readerID := s.readerID()
		_ = db.UpsertCheckpoint(ctx, readerID, applied)
	}
}

func (s *Set) readerID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s@%d", host, os.Getpid())
}

// migrateV1ToV2 loads v1 tree and .dat, re-encodes values into the ValueStore, and
// persists back as v2.
func (s *Set) migrateV1ToV2(r *bintly.Reader) error {
	// No-op: migration disabled. Kept for compatibility; never called in new flow.
	return nil
}

// wipeOldStore removes legacy v1 index and data files for a clean start.
func (s *Set) wipeOldStore() error {
	oldTre := filepath.Join(s.baseURL, fmt.Sprintf("index_%s.tre", s.name))
	oldDat := filepath.Join(s.baseURL, fmt.Sprintf("index_%s.dat", s.name))
	log.Printf("vectordb/mem: detected legacy store for namespace=%s; removing %s and %s", s.name, oldTre, oldDat)
	if err := os.Remove(oldTre); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("vectordb/mem: failed removing %s: %v", oldTre, err)
	}
	if err := os.Remove(oldDat); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("vectordb/mem: failed removing %s: %v", oldDat, err)
	}
	// remove potential leftover data dir
	oldDataDir := filepath.Join(s.baseURL, "data", s.name)
	// best-effort remove files in directory, then dir
	if entries, err := os.ReadDir(oldDataDir); err == nil {
		for _, e := range entries {
			p := filepath.Join(oldDataDir, e.Name())
			if err := os.Remove(p); err != nil {
				log.Printf("vectordb/mem: failed removing data file %s: %v", p, err)
			}
		}
		if err := os.Remove(oldDataDir); err != nil {
			log.Printf("vectordb/mem: failed removing data dir %s: %v", oldDataDir, err)
		}
	}
	// reset in-memory
	s.mu.Lock()
	s.vectors = nil
	s.ptrs = nil
	s.alive = nil
	s.mu.Unlock()
	return nil
}

// close releases resources (value store + lock file) for this set.
func (s *Set) close() error {
	var firstErr error
	// stop tailer
	if s.tailCancel != nil {
		s.tailCancel()
		s.tailWG.Wait()
		s.tailCancel = nil
	}
	if s.values != nil {
		if err := s.values.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.lockFile != nil {
		// unlocking is best-effort; closing also releases the lock
		_ = unlockFile(s.lockFile)
		if err := s.lockFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.lockFile = nil
	}
	return firstErr
}

func (s *Set) startTailer() {
	ctx, cancel := context.WithCancel(context.Background())
	s.tailCancel = cancel
	s.tailWG.Add(1)
	interval := s.tailInterval
	go func() {
		defer s.tailWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.syncFromJournal(ctx)
			}
		}
	}()
}

// enterWriterGate participates in a simple file-based queue to fairly serialize
// multi-process writers. Each process creates a ticket in dataDir/gate and waits
// until its ticket is lexicographically first, then proceeds to acquire writer.lock.
func (s *Set) enterWriterGate(ctx context.Context, dataDir string) error {
	gateDir := filepath.Join(dataDir, "gate")
	if err := os.MkdirAll(gateDir, 0o755); err != nil {
		return err
	}
	// Create ticket
	ticket := fmt.Sprintf("%020d_%06d", time.Now().UnixNano(), os.Getpid())
	ticketPath := filepath.Join(gateDir, ticket)
	if err := os.WriteFile(ticketPath, []byte(""), 0o644); err != nil {
		return fmt.Errorf("gate: create ticket: %w", err)
	}
	// Ensure cleanup on error
	cleanup := func() { _ = os.Remove(ticketPath) }
	// Wait loop
	for {
		select {
		case <-ctx.Done():
			cleanup()
			return ctx.Err()
		case <-time.After(s.writerGatePoll):
		}
		// List tickets
		entries, err := os.ReadDir(gateDir)
		if err != nil {
			cleanup()
			return fmt.Errorf("gate: readdir: %w", err)
		}
		// Reap stale tickets
		now := time.Now()
		var names []string
		for _, e := range entries {
			name := e.Name()
			info, _ := e.Info()
			if info != nil && now.Sub(info.ModTime()) > s.writerGateTTL {
				_ = os.Remove(filepath.Join(gateDir, name))
				continue
			}
			names = append(names, name)
		}
		if len(names) == 0 {
			// our ticket got reaped unexpectedly; recreate and continue
			_ = os.WriteFile(ticketPath, []byte(""), 0o644)
			continue
		}
		sort.Strings(names)
		if names[0] == ticket {
			// Our turn; remove our ticket and proceed
			cleanup()
			return nil
		}
	}
}

// AddDocuments embeds and adds documents to the set. The document payloads are
// encoded and written to the ValueStore; the in-memory tree stores only Ptrs.
func (s *Set) AddDocuments(ctx context.Context, docs []schema.Document, opts ...vectorstores.Option) ([]string, error) {
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.Embedder == nil {
		return nil, fmt.Errorf("embedder is required")
	}

	// Prepare embedding input
	texts := make([]string, 0, len(docs))
	for i := range docs {
		texts = append(texts, docs[i].PageContent)
	}
	vecs, err := options.Embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(vecs) != len(docs) {
		return nil, fmt.Errorf("embedder returned %d vectors, expected %d", len(vecs), len(docs))
	}

	writers := bintly.NewWriters()

	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, len(docs))
	for i := range docs {
		// Encode document
		var doc vdb.Document = vdb.Document(docs[i])
		writer := writers.Get()
		if err := doc.EncodeBinary(writer); err != nil {
			writers.Put(writer)
			return nil, err
		}
		bs := writer.Bytes()
		writers.Put(writer)

		// Store externally and keep pointer
		ptr, err := s.values.Append(bs)
		if err != nil {
			return nil, err
		}

		// Append to in-memory arrays
		s.ptrs = append(s.ptrs, ptr)
		s.vectors = append(s.vectors, vecs[i])
		s.alive = append(s.alive, true)
		newIdx := len(s.ptrs) - 1
		ids[i] = strconv.Itoa(newIdx)
		// update cover index if enabled
		if s.tree != nil && len(vecs[i]) > 0 {
			pt := cover.NewPoint(vecs[i]...)
			_ = s.tree.Insert(newIdx, pt)
		}
	}
	return ids, nil
}

// SimilaritySearch queries the set using the cover-tree index (kNN)
// and materializes only the top-K documents via the ValueStore.
func (s *Set) SimilaritySearch(ctx context.Context, query string, k int, opts ...vectorstores.Option) ([]schema.Document, error) {
	options := vectorstores.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.Embedder == nil {
		return nil, fmt.Errorf("embedder is required")
	}
	qvec, err := options.Embedder.EmbedQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	// Cover-tree only path
	if s.tree == nil {
		return nil, fmt.Errorf("cover index not initialized")
	}
	pt := cover.NewPoint(qvec...)
	// Over-fetch to account for filtered (dead) entries
	candK := k
	if candK < 32 {
		candK = k * 2
	}
	// Prefer best-first if available in current gds; attempt via type assertion
	// Note: our imported Tree exposes BestFirst in newer versions; call if present.
	neighbors := s.tree.KNearestNeighbors(pt, candK)
	indices := make([]int, 0, k)
	scores := make([]float32, 0, k)
	s.mu.RLock()
	for _, n := range neighbors {
		idx := s.tree.Value(n.Point)
		if idx < 0 || idx >= len(s.alive) {
			continue
		}
		if !s.alive[idx] {
			continue
		}
		indices = append(indices, idx)
		var sim float32
		if s.coverDist == cover.DistanceFunctionCosine {
			sim = 1 - n.Distance
		} else {
			sim = -n.Distance
		}
		scores = append(scores, sim)
		if len(indices) == k {
			break
		}
	}
	s.mu.RUnlock()
	return s.materializeTop(ctx, indices, scores)
}

// materializeTop fetches documents by indices and assigns their scores.
func (s *Set) materializeTop(ctx context.Context, indices []int, scores []float32) ([]schema.Document, error) {
	results := make([]schema.Document, 0, len(indices))
	for i, idx := range indices {
		s.mu.RLock()
		ptr := s.ptrs[idx]
		s.mu.RUnlock()
		bs, err := s.values.Read(ptr)
		if err != nil {
			return nil, err
		}
		readers := bintly.NewReaders()
		r := readers.Get()
		_ = r.FromBytes(bs)
		var vdoc vdb.Document
		if err := vdoc.DecodeBinary(r); err != nil {
			readers.Put(r)
			return nil, err
		}
		readers.Put(r)
		doc := schema.Document(vdoc)
		if i < len(scores) {
			doc.Score = scores[i]
		}
		results = append(results, doc)
	}
	return results, nil
}

// Remove marks a document as removed and appends a tombstone to the ValueStore.
func (s *Set) Remove(ctx context.Context, id string, _ ...vectorstores.Option) error {
	idx, err := strconv.Atoi(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx < 0 || idx >= len(s.alive) {
		return fmt.Errorf("invalid id: %s", id)
	}
	if !s.alive[idx] {
		return nil
	}
	// best-effort tombstone; ignore error to keep in-memory state consistent
	_ = s.values.Delete(s.ptrs[idx])
	s.alive[idx] = false
	// keep cover index consistent when enabled (best-effort)
	if s.tree != nil {
		if pt := s.tree.FindPointByIndex(int32(idx)); pt != nil {
			_ = s.tree.Remove(pt)
		}
	}
	return nil
}
