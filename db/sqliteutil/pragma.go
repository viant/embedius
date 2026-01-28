package sqliteutil

import (
	"fmt"
	"strings"
)

// EnsurePragmas appends SQLite pragmas to the DSN when missing.
// It is a no-op for in-memory databases.
func EnsurePragmas(dsn string, wal bool, busyTimeoutMS int) string {
	if dsn == "" {
		return dsn
	}
	lower := strings.ToLower(dsn)
	if dsn == ":memory:" || strings.HasPrefix(lower, "file::memory:") {
		return dsn
	}
	if wal && !strings.Contains(lower, "_pragma=journal_mode") {
		dsn = addPragma(dsn, "journal_mode(WAL)")
	}
	if busyTimeoutMS > 0 && !strings.Contains(lower, "_pragma=busy_timeout") {
		dsn = addPragma(dsn, fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	}
	return dsn
}

func addPragma(dsn, pragma string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "_pragma=" + pragma
}
