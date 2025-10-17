package storage

// Ptr identifies a value stored in external data files.
// It is a lightweight pointer: (segmentID, offset, length).
//
// SegmentID: logical segment number within a namespace.
// Offset: byte offset within the segment file where the payload begins.
// Length: payload length in bytes (not including record headers/CRC/padding).
type Ptr struct {
	SegmentID uint32 `json:"segmentId"`
	Offset    uint64 `json:"offset"`
	Length    uint32 `json:"length"`
}

// Stats exposes basic runtime and storage metrics.
type Stats struct {
	// Logical number of records appended (including tombstones)
	Appends uint64 `json:"appends"`
	// Total bytes written to data segments (including headers/padding)
	BytesWritten uint64 `json:"bytesWritten"`
	// Total bytes read from data segments
	BytesRead uint64 `json:"bytesRead"`
	// Number of segments tracked by the store
	Segments int `json:"segments"`
	// Estimated live vs. dead space (if available)
	LiveBytes uint64 `json:"liveBytes,omitempty"`
	DeadBytes uint64 `json:"deadBytes,omitempty"`
}

// ValueStore defines an append-only, segment-based value storage.
// Implementations should be safe for concurrent reads and a single writer
// per namespace (process-level coordination recommended).
type ValueStore interface {
	// Append writes a new value and returns its pointer. The returned Ptr is
	// stable for the lifetime of the underlying data files.
	Append(value []byte) (Ptr, error)

	// Read loads the value located at the provided pointer.
	Read(ptr Ptr) ([]byte, error)

	// Delete marks a value as deleted (e.g. by appending a tombstone).
	// Space reclamation is implementation-defined (compaction).
	Delete(ptr Ptr) error

	// Sync flushes in-memory state to stable storage (data + manifest).
	Sync() error

	// Close releases resources (mmaps, file handles). After Close, the
	// implementation must be unusable and should return ErrClosed.
	Close() error

	// Stats returns best-effort metrics; it should be cheap to call.
	Stats() Stats
}
