package mem

import (
	vstorage "github.com/viant/embedius/vectordb/storage"
	"time"
)

type StoreOption func(s *Store)

func WithBaseURL(baseURL string) StoreOption {
	return func(s *Store) {
		s.baseURL = baseURL
	}
}

// WithSetValueStore sets a default ValueStore used by all newly created Sets.
func WithSetValueStore(v vstorage.ValueStore) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, WithValueStore(v))
	}
}

type SetOption func(s *Set)

// WithValueStore injects a custom ValueStore into a Set.
func WithValueStore(v vstorage.ValueStore) SetOption {
	return func(s *Set) { s.values = v }
}

// WithExternalValues toggles using external value storage (mmap-backed) by default
// when a baseURL is configured. If set to false, an in-memory ValueStore is used
// even if baseURL is set.
func WithExternalValues(enabled bool) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.externalValues = enabled })
	}
}

// WithSegmentSize configures the mmap-backed ValueStore segment size (bytes).
// Applies to newly created Sets.
func WithSegmentSize(bytes int64) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.segmentSize = bytes })
	}
}

// WithSetExternalValues is the Set-level variant of WithExternalValues.
func WithSetExternalValues(enabled bool) SetOption {
	return func(s *Set) { s.externalValues = enabled }
}

// WithSetSegmentSize is the Set-level variant of WithSegmentSize.
func WithSetSegmentSize(bytes int64) SetOption {
	return func(s *Set) { s.segmentSize = bytes }
}

// WithWriterQueue enables a file-based queued writer gate for multi-process writers.
// Writers will create a ticket and wait for their turn before acquiring the lock.
func WithWriterQueue(enabled bool) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.writerQueue = enabled })
	}
}

// WithSetWriterQueue is the Set-level variant of WithWriterQueue.
func WithSetWriterQueue(enabled bool) SetOption {
	return func(s *Set) { s.writerQueue = enabled }
}

// WithWriterGatePoll sets the poll interval for the writer gate queue.
func WithWriterGatePoll(interval time.Duration) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.writerGatePoll = interval })
	}
}

// WithSetWriterGatePoll sets the poll interval for a specific Set.
func WithSetWriterGatePoll(interval time.Duration) SetOption {
	return func(s *Set) { s.writerGatePoll = interval }
}

// WithWriterGateTTL sets the TTL for queue tickets; stale tickets are reaped.
func WithWriterGateTTL(ttl time.Duration) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.writerGateTTL = ttl })
	}
}

// WithSetWriterGateTTL sets the TTL for tickets at the Set level.
func WithSetWriterGateTTL(ttl time.Duration) SetOption {
	return func(s *Set) { s.writerGateTTL = ttl }
}

// WithJobQueue enables the SQLite-backed coordination job queue for a Store (applies to new Sets).
func WithJobQueue(enabled bool) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.jobQueue = enabled })
	}
}

// WithSetJobQueue enables the job queue for a specific Set.
func WithSetJobQueue(enabled bool) SetOption {
	return func(s *Set) { s.jobQueue = enabled }
}

// WithCoordPath sets a custom path for the coordination DB (e.g., SQLite) under a Store.
func WithCoordPath(path string) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.coordPath = path })
	}
}

// WithSetCoordPath sets a custom path for the coordination DB for a specific Set.
func WithSetCoordPath(path string) SetOption {
	return func(s *Set) { s.coordPath = path }
}

// WithReadOnly opens Sets in read-only mode (no writer lock acquisition).
func WithReadOnly(enabled bool) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) { set.readOnly = enabled })
	}
}

// WithSetReadOnly is the Set-level variant of WithReadOnly.
func WithSetReadOnly(enabled bool) SetOption {
	return func(s *Set) { s.readOnly = enabled }
}

// WithJournalTTL sets the journal retention TTL used by the writer service.
func WithJournalTTL(d time.Duration) StoreOption {
	return func(s *Store) { s.setOptions = append(s.setOptions, func(set *Set) { set.journalTTL = d }) }
}

// WithSetJournalTTL is the Set-level variant of WithJournalTTL.
func WithSetJournalTTL(d time.Duration) SetOption { return func(s *Set) { s.journalTTL = d } }

// WithStaleReaderTTL sets the staleness TTL for reader checkpoints.
func WithStaleReaderTTL(d time.Duration) StoreOption {
	return func(s *Store) { s.setOptions = append(s.setOptions, func(set *Set) { set.staleReaderTTL = d }) }
}

// WithSetStaleReaderTTL is the Set-level variant of WithStaleReaderTTL.
func WithSetStaleReaderTTL(d time.Duration) SetOption { return func(s *Set) { s.staleReaderTTL = d } }

// WithWriterBatch sets the writer claim/apply batch size.
func WithWriterBatch(n int) StoreOption {
	return func(s *Store) { s.setOptions = append(s.setOptions, func(set *Set) { set.writerBatch = n }) }
}

// WithSetWriterBatch is the Set-level variant of WithWriterBatch.
func WithSetWriterBatch(n int) SetOption { return func(s *Set) { s.writerBatch = n } }

// WithTailInterval sets the background journal tail interval for all new Sets.
// If zero or negative, the tailer is disabled.
func WithTailInterval(d time.Duration) StoreOption {
	return func(s *Store) { s.setOptions = append(s.setOptions, func(set *Set) { set.tailInterval = d }) }
}

// WithSetTailInterval sets the background journal tail interval for a specific Set.
func WithSetTailInterval(d time.Duration) SetOption { return func(s *Set) { s.tailInterval = d } }

// WithWriterLeaseTTL configures the default writer lease TTL at the Store level.
func WithWriterLeaseTTL(d time.Duration) StoreOption {
	return func(s *Store) { s.writerLeaseTTL = d }
}

// WriterMetrics allows plugging metrics hooks for writer events.
type WriterMetrics interface {
	OnLeaseAcquired(namespace, owner string, ttl time.Duration)
	OnLeaseReleased(namespace, owner, reason string)
	OnLeaseLost(namespace, owner string)
	OnBatchApplied(namespace string, jobs, adds, removes int)
	OnError(namespace, where string, err error)
}

// WithWriterMetrics sets a metrics sink for writer events.
func WithWriterMetrics(m WriterMetrics) StoreOption {
	return func(s *Store) { s.writerMetrics = m }
}

// WithWriterLock configures writer lock behavior for all new Sets.
// If blocking is true and timeout <= 0, lock waits indefinitely.
// If blocking is true and timeout > 0, lock retries until timeout, then errors.
// If blocking is false, lock is non-blocking and errors immediately if busy.
func WithWriterLock(blocking bool, timeout time.Duration) StoreOption {
	return func(s *Store) {
		s.setOptions = append(s.setOptions, func(set *Set) {
			set.lockBlocking = blocking
			set.lockTimeout = timeout
		})
	}
}

// WithSetWriterLock applies writer lock behavior to a specific Set.
func WithSetWriterLock(blocking bool, timeout time.Duration) SetOption {
	return func(s *Set) {
		s.lockBlocking = blocking
		s.lockTimeout = timeout
	}
}
