package sqlite

import "errors"

var (
	// ErrNotEnabled is returned when the sqlite build tag is not enabled.
	ErrNotEnabled = errors.New("coord/sqlite: not enabled (build with -tags sqlite)")
)
