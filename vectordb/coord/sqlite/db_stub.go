//go:build no_sqlite
// +build no_sqlite

package sqlite

import (
	"context"
	"time"
)

// DB is a stub when built without the sqlite tag.
type DB struct{}

// Open returns ErrNotEnabled unless built with -tags sqlite.
func Open(_ string) (*DB, error) { return nil, ErrNotEnabled }

func (d *DB) EnsureSchema(_ context.Context) error         { return ErrNotEnabled }
func (d *DB) GetLastSCN(_ context.Context) (uint64, error) { return 0, ErrNotEnabled }
func (d *DB) SetLastSCN(_ context.Context, _ uint64) error { return ErrNotEnabled }
func (d *DB) EnqueueJob(_ context.Context, _ string, _ []byte, _, _ string) (int64, error) {
	return 0, ErrNotEnabled
}

type Job struct {
	ID      int64
	Kind    string
	Payload []byte
}

func (d *DB) ClaimPending(_ context.Context, _ string, _ int) ([]Job, error) {
	return nil, ErrNotEnabled
}
func (d *DB) MarkJobsDone(_ context.Context, _ []int64) error { return ErrNotEnabled }

type Event struct {
	Kind, ID string
	Seg      uint32
	Off      uint64
	Len      uint32
	Vec      []float32
}

func (d *DB) InsertEvents(_ context.Context, _ []Event) (uint64, error) { return 0, ErrNotEnabled }

type EventRow struct {
	SCN uint64
	Ev  Event
}

func (d *DB) EventsSince(_ context.Context, _ uint64) ([]EventRow, error)  { return nil, ErrNotEnabled }
func (d *DB) UpsertCheckpoint(_ context.Context, _ string, _ uint64) error { return ErrNotEnabled }
func (d *DB) SetJobResult(_ context.Context, _ string, _ []byte) error     { return ErrNotEnabled }
func (d *DB) GetJobResult(_ context.Context, _ string) ([]byte, string, error) {
	return nil, "", ErrNotEnabled
}
func (d *DB) TryAcquireWriter(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return false, ErrNotEnabled
}
func (d *DB) RenewWriter(_ context.Context, _ string) (bool, error) { return false, ErrNotEnabled }
func (d *DB) ReleaseWriter(_ context.Context, _ string) error       { return ErrNotEnabled }
func (d *DB) Close() error                                          { return nil }
