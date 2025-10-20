package mmapstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/viant/embedius/vectordb/storage"
)

// Implementation notes
// - This is an append-only value store with segmented files.
// - It does not rely on actual OS mmap for writes to keep it portable and simple.
//   Reads use direct file I/O as well; a future optimization can add read-only mmaps.
// - Each record layout: [kind:1][len:uvarint][payload:len][crc32:4]
//   Ptr.Offset points at the start of payload (after kind+len header).

const (
	kindDoc       = 0x01
	kindTombstone = 0xFE
	manifestName  = "manifest.json"
)

// Options configures the store.
type Options struct {
	// BasePath is the directory where segment files and manifest are stored.
	BasePath string
	// SegmentSize is the soft limit for each segment file before rotation.
	SegmentSize int64
}

// remap is implemented in OS-specific files.

// read returns the payload bytes by either reading from mmap view (if available)
// or falling back to direct file I/O.
func (seg *segment) read(ptr storage.Ptr) ([]byte, error) {
	end := int64(ptr.Offset) + int64(ptr.Length) + 4
	if seg.data != nil && end <= int64(len(seg.data)) {
		// payload is fully inside mapped region
		start := int(ptr.Offset)
		payload := make([]byte, int(ptr.Length))
		copy(payload, seg.data[start:start+int(ptr.Length)])
		want := binary.LittleEndian.Uint32(seg.data[start+int(ptr.Length) : start+int(ptr.Length)+4])
		got := crc32.ChecksumIEEE(payload)
		if want != got {
			return nil, storage.ErrCorrupt
		}
		return payload, nil
	}
	// fallback to ReadAt
	buf := make([]byte, int(ptr.Length))
	if _, err := seg.f.ReadAt(buf, int64(ptr.Offset)); err != nil {
		return nil, err
	}
	var crcBuf [4]byte
	if _, err := seg.f.ReadAt(crcBuf[:], int64(ptr.Offset)+int64(ptr.Length)); err != nil {
		return nil, err
	}
	want := binary.LittleEndian.Uint32(crcBuf[:])
	got := crc32.ChecksumIEEE(buf)
	if want != got {
		return nil, storage.ErrCorrupt
	}
	return buf, nil
}

func (o *Options) withDefaults() {
	if o.SegmentSize <= 0 {
		// default 1 GiB
		o.SegmentSize = 1 << 30
	}
}

// Open creates or opens a Store at the provided base path.
func Open(opts Options) (*Store, error) {
	opts.withDefaults()
	if opts.BasePath == "" {
		return nil, fmt.Errorf("mmapstore: BasePath is required")
	}
	if err := os.MkdirAll(opts.BasePath, 0o755); err != nil {
		return nil, fmt.Errorf("mmapstore: mkdir: %w", err)
	}
	st := &Store{
		basePath:    opts.BasePath,
		segmentSize: opts.SegmentSize,
		segments:    map[uint32]*segment{},
	}
	if err := st.loadOrInit(); err != nil {
		_ = st.Close()
		return nil, err
	}
	return st, nil
}

// Store implements storage.ValueStore.
type Store struct {
	mu          sync.RWMutex
	basePath    string
	segmentSize int64
	manifest    manifest
	segments    map[uint32]*segment
	active      *segment
	closed      bool

	stats storage.Stats
}

type manifest struct {
	Version     int           `json:"version"`
	CreatedAt   time.Time     `json:"createdAt"`
	SegmentSize int64         `json:"segmentSize"`
	NextSegID   uint32        `json:"nextSegId"`
	Active      uint32        `json:"active"`
	Segments    []segmentMeta `json:"segments"`
}

type segmentMeta struct {
	ID   uint32 `json:"id"`
	Size int64  `json:"size"`
	Tail int64  `json:"tail"`
}

type segment struct {
	id   uint32
	path string
	f    *os.File
	size int64 // physical file size
	tail int64 // logical end of valid data

	// mmap-backed readonly view; may be nil if mapping is unsupported
	data []byte
}

func (s *Store) loadOrInit() error {
	manPath := filepath.Join(s.basePath, manifestName)
	if _, err := os.Stat(manPath); errors.Is(err, os.ErrNotExist) {
		// initialize new store with segment 0
		seg, err := s.openOrCreateSegment(0)
		if err != nil {
			return err
		}
		s.segments[0] = seg
		s.active = seg
		s.manifest = manifest{
			Version:     1,
			CreatedAt:   time.Now(),
			SegmentSize: s.segmentSize,
			NextSegID:   1,
			Active:      0,
			Segments:    []segmentMeta{{ID: 0, Size: seg.size, Tail: seg.tail}},
		}
		return s.persistManifest()
	}
	// load manifest
	f, err := os.Open(manPath)
	if err != nil {
		return fmt.Errorf("mmapstore: open manifest: %w", err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	if err := dec.Decode(&s.manifest); err != nil {
		return fmt.Errorf("mmapstore: decode manifest: %w", err)
	}
	// open segments and recover tails
	for _, m := range s.manifest.Segments {
		seg, err := s.openOrCreateSegment(m.ID)
		if err != nil {
			return err
		}
		// recovery scan to find tail in case of unflushed writes or crash
		if err := seg.recoverTail(); err != nil {
			return err
		}
		s.segments[m.ID] = seg
	}
	act := s.manifest.Active
	s.active = s.segments[act]
	if s.active == nil {
		return fmt.Errorf("mmapstore: active segment %d missing", act)
	}
	return nil
}

func (s *Store) persistManifest() error {
	tmp := filepath.Join(s.basePath, ".manifest.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&s.manifest); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	manPath := filepath.Join(s.basePath, manifestName)
	return os.Rename(tmp, manPath)
}

func (s *Store) segmentPath(id uint32) string {
	return filepath.Join(s.basePath, fmt.Sprintf("data_%06d.vdat", id))
}

func (s *Store) openOrCreateSegment(id uint32) (*segment, error) {
	path := s.segmentPath(id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("mmapstore: open segment: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	seg := &segment{id: id, path: path, f: f, size: info.Size(), tail: info.Size()}
	// best-effort mmap of current size; ignore errors (fallback to ReadAt)
	_ = seg.remap()
	return seg, nil
}

func (seg *segment) recoverTail() error {
	size := seg.size
	var off int64
	buf := make([]byte, 16)
	for off < size {
		// read kind
		n, err := seg.f.ReadAt(buf[:1], off)
		if err != nil {
			if errors.Is(err, io.EOF) || n == 0 {
				break
			}
			return fmt.Errorf("recover: read kind at %d: %w", off, err)
		}
		kind := buf[0]
		// read uvarint length
		// We attempt to read up to 10 bytes for uvarint
		maxVar := 10
		if off+1+int64(maxVar) > size {
			maxVar = int(size - off - 1)
		}
		if maxVar <= 0 {
			break
		}
		if _, err := seg.f.ReadAt(buf[:maxVar], off+1); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("recover: read len at %d: %w", off+1, err)
		}
		l, nlen := binary.Uvarint(buf[:maxVar])
		if nlen <= 0 {
			// invalid varint; stop
			break
		}
		recBody := int64(l)
		recLen := int64(1+nlen) + recBody + 4 // kind + varint + payload + crc32
		next := off + recLen
		if next > size {
			// truncated record
			break
		}
		// verify crc
		// read payload into buf chunk-by-chunk to compute crc; here we read directly if small
		// but for recovery we can skip heavy validation to keep it fast; still, we try.
		// Read payload
		payload := make([]byte, recBody)
		if recBody > 0 {
			if _, err := seg.f.ReadAt(payload, off+int64(1+nlen)); err != nil {
				break
			}
		}
		// read stored crc
		if _, err := seg.f.ReadAt(buf[:4], off+int64(1+nlen)+recBody); err != nil {
			break
		}
		want := binary.LittleEndian.Uint32(buf[:4])
		got := crc32.ChecksumIEEE(payload)
		if want != got {
			// corruption; stop before this record
			break
		}
		// record ok; advance
		_ = kind // not used; could track live/dead space later
		off = next
	}
	// truncate trailing garbage
	if off < size {
		if err := seg.f.Truncate(off); err != nil {
			return err
		}
	}
	seg.size = off
	seg.tail = off
	// refresh mmap to the logical end
	_ = seg.remap()
	return nil
}

// Append implements storage.ValueStore.Append.
func (s *Store) Append(value []byte) (storage.Ptr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.Ptr{}, storage.ErrClosed
	}
	if err := s.ensureCapacity(int64(recordOverhead(len(value)))); err != nil {
		return storage.Ptr{}, err
	}
	seg := s.active
	// prepare header
	header := make([]byte, 1+binary.MaxVarintLen64)
	header[0] = kindDoc
	nlen := binary.PutUvarint(header[1:], uint64(len(value)))
	header = header[:1+nlen]
	// write header
	off := seg.tail
	if _, err := seg.f.WriteAt(header, off); err != nil {
		return storage.Ptr{}, err
	}
	off += int64(len(header))
	// write payload
	if len(value) > 0 {
		if _, err := seg.f.WriteAt(value, off); err != nil {
			return storage.Ptr{}, err
		}
	}
	// write crc
	crc := crc32.ChecksumIEEE(value)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := seg.f.WriteAt(crcBuf[:], off+int64(len(value))); err != nil {
		return storage.Ptr{}, err
	}
	// advance tail/size
	recLen := int64(len(header) + len(value) + 4)
	seg.tail += recLen
	if seg.tail > seg.size {
		seg.size = seg.tail
	}
	// update manifest meta in-memory
	s.updateSegmentMetaLocked(seg)

	s.stats.Appends++
	s.stats.BytesWritten += uint64(recLen)
	s.stats.LiveBytes += uint64(len(value))
	s.stats.Segments = len(s.segments)
	return storage.Ptr{SegmentID: seg.id, Offset: uint64(off), Length: uint32(len(value))}, nil
}

func (s *Store) ensureCapacity(needed int64) error {
	seg := s.active
	if seg == nil {
		return fmt.Errorf("mmapstore: no active segment")
	}
	// approximate capacity: rotate if tail + needed + 1 MiB slack exceeds segment size
	if seg.tail+needed > s.segmentSize {
		// rotate
		id := s.manifest.NextSegID
		newSeg, err := s.openOrCreateSegment(id)
		if err != nil {
			return err
		}
		if err := newSeg.recoverTail(); err != nil {
			_ = newSeg.f.Close()
			return err
		}
		s.segments[id] = newSeg
		s.active = newSeg
		s.manifest.Active = id
		s.manifest.NextSegID = id + 1
		s.manifest.Segments = append(s.manifest.Segments, segmentMeta{ID: id, Size: newSeg.size, Tail: newSeg.tail})
		// persist manifest on rotation to make it durable
		if err := s.persistManifest(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) updateSegmentMetaLocked(seg *segment) {
	for i := range s.manifest.Segments {
		if s.manifest.Segments[i].ID == seg.id {
			s.manifest.Segments[i].Size = seg.size
			s.manifest.Segments[i].Tail = seg.tail
			return
		}
	}
}

// Read implements storage.ValueStore.Read.
func (s *Store) Read(ptr storage.Ptr) ([]byte, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, storage.ErrClosed
	}
	seg := s.segments[ptr.SegmentID]
	s.mu.RUnlock()
	if seg == nil {
		return nil, storage.ErrInvalidPtr
	}
	if ptr.Length == 0 {
		return []byte{}, nil
	}
	buf, err := seg.read(ptr)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.stats.BytesRead += uint64(len(buf) + 4)
	s.mu.Unlock()
	return buf, nil
}

// Delete implements storage.ValueStore.Delete by appending a tombstone.
func (s *Store) Delete(ptr storage.Ptr) error {
	// encode tombstone payload as little-endian segID|offset|length
	payload := make([]byte, 4+8+4)
	binary.LittleEndian.PutUint32(payload[:4], ptr.SegmentID)
	binary.LittleEndian.PutUint64(payload[4:12], ptr.Offset)
	binary.LittleEndian.PutUint32(payload[12:16], ptr.Length)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.ErrClosed
	}
	if err := s.ensureCapacity(int64(recordOverhead(len(payload)))); err != nil {
		return err
	}
	seg := s.active
	header := make([]byte, 1+binary.MaxVarintLen64)
	header[0] = kindTombstone
	nlen := binary.PutUvarint(header[1:], uint64(len(payload)))
	header = header[:1+nlen]

	off := seg.tail
	if _, err := seg.f.WriteAt(header, off); err != nil {
		return err
	}
	off += int64(len(header))
	if _, err := seg.f.WriteAt(payload, off); err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(payload)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := seg.f.WriteAt(crcBuf[:], off+int64(len(payload))); err != nil {
		return err
	}
	recLen := int64(len(header) + len(payload) + 4)
	seg.tail += recLen
	if seg.tail > seg.size {
		seg.size = seg.tail
	}
	s.updateSegmentMetaLocked(seg)
	s.stats.Appends++
	s.stats.BytesWritten += uint64(recLen)
	s.stats.DeadBytes += uint64(len(payload))
	s.stats.Segments = len(s.segments)
	return nil
}

// Sync flushes data and manifest to disk.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.ErrClosed
	}
	for _, seg := range s.segments {
		if seg.f != nil {
			if err := seg.f.Sync(); err != nil {
				return err
			}
		}
	}
	return s.persistManifest()
}

// Close closes all segment files.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	for _, seg := range s.segments {
		if seg.f != nil {
			if err := seg.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		seg.unmap()
	}
	return firstErr
}

// Stats returns best-effort metrics.
func (s *Store) Stats() storage.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func recordOverhead(payloadLen int) int {
	return 1 + uvarintLen(uint64(payloadLen)) + payloadLen + 4
}

func uvarintLen(v uint64) int {
	switch {
	case v < 1<<7:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<21:
		return 3
	case v < 1<<28:
		return 4
	case v < 1<<35:
		return 5
	case v < 1<<42:
		return 6
	case v < 1<<49:
		return 7
	case v < 1<<56:
		return 8
	case v < 1<<63:
		return 9
	default:
		return 10
	}
}
