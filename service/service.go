package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/viant/embedius/db/sqliteutil"
	"github.com/viant/embedius/embeddings"
	"github.com/viant/sqlite-vec/engine"
	"github.com/viant/sqlite-vec/vec"
	"github.com/viant/sqlite-vec/vecadmin"
)

// Option configures the Service.
type Option func(*Service)

// WithDB sets an existing database handle.
func WithDB(db *sql.DB) Option {
	return func(s *Service) { s.db = db }
}

// WithDSN sets the SQLite DSN used when opening a DB.
func WithDSN(dsn string) Option {
	return func(s *Service) { s.dsn = dsn }
}

// WithDriver sets the SQL driver for the store (e.g., "sqlite", "mysql").
func WithDriver(driver string) Option {
	return func(s *Service) { s.driver = strings.TrimSpace(driver) }
}

// WithEmbedder sets the default embedder.
func WithEmbedder(embedder embeddings.Embedder) Option {
	return func(s *Service) { s.embedder = embedder }
}

// Service exposes reusable operations for indexing, search, sync, and admin.
type Service struct {
	db       *sql.DB
	dsn      string
	driver   string
	embedder embeddings.Embedder
	mu       sync.Mutex
}

// NewService creates a new Service.
func NewService(opts ...Option) (*Service, error) {
	s := &Service{}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Close releases an owned DB connection (if any).
func (s *Service) Close() error {
	if s.db != nil && s.dsn != "" {
		return s.db.Close()
	}
	return nil
}

func (s *Service) ensureDB(ctx context.Context, dsn string, withAdmin bool) (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dsn == "" {
		dsn = s.dsn
	}
	if s.driver == "" {
		s.driver = detectDriverFromDSN(dsn)
	}
	if s.driver == "" {
		s.driver = "sqlite"
	}
	if s.db != nil {
		if s.driver == "sqlite" {
			if err := vec.Register(s.db); err != nil {
				return nil, err
			}
			if withAdmin {
				if err := vecadmin.Register(s.db); err != nil {
					return nil, err
				}
			}
		}
		return s.db, nil
	}
	if dsn == "" {
		return nil, fmt.Errorf("store dsn required")
	}
	var bd *sql.DB
	if s.driver == "sqlite" {
		var err error
		bd, err = engine.Open(sqliteutil.EnsurePragmas(dsn, true, 5000))
		if err != nil {
			return nil, err
		}
		bd.SetConnMaxLifetime(0)
		bd.SetConnMaxIdleTime(0)
		if err := vec.Register(bd); err != nil {
			_ = bd.Close()
			return nil, err
		}
		if withAdmin {
			if err := vecadmin.Register(bd); err != nil {
				_ = bd.Close()
				return nil, err
			}
		}
		// Force-create a connection after module registration.
		if conn, err := bd.Conn(ctx); err == nil {
			_ = conn.Close()
		}
	} else {
		var err error
		bd, err = sql.Open(s.driver, dsn)
		if err != nil {
			return nil, err
		}
	}
	s.db = bd
	return bd, nil
}

func (s *Service) resolveEmbedder(override embeddings.Embedder) (embeddings.Embedder, error) {
	if override != nil {
		return override, nil
	}
	if s.embedder != nil {
		return s.embedder, nil
	}
	return nil, fmt.Errorf("embedder is required")
}

func ensureSchemaConn(ctx context.Context, db *sql.DB, driver string) (*sql.Conn, error) {
	if strings.TrimSpace(driver) == "" {
		driver = "sqlite"
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if driver == "sqlite" {
		conn, err = ensureVecModule(ctx, db, conn)
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return nil, err
		}
	}
	if err := ensureSchema(ctx, conn, driver); err != nil {
		if driver == "sqlite" && strings.Contains(err.Error(), "no such module: vec") {
			_ = conn.Close()
			if err := vec.Register(db); err != nil {
				return nil, err
			}
			conn, err = db.Conn(ctx)
			if err != nil {
				return nil, err
			}
			conn, err = ensureVecModule(ctx, db, conn)
			if err != nil {
				if conn != nil {
					_ = conn.Close()
				}
				return nil, err
			}
			if err := ensureSchema(ctx, conn, driver); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		}
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func ensureVecModule(ctx context.Context, db *sql.DB, conn *sql.Conn) (*sql.Conn, error) {
	available, err := checkVecModule(ctx, conn)
	if err != nil {
		return nil, err
	}
	if available {
		return conn, nil
	}
	_ = conn.Close()
	if err := vec.Register(db); err != nil {
		return nil, err
	}
	conn, err = db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	available, err = checkVecModule(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if available {
		return conn, nil
	}
	// Unable to validate module availability; proceed and let callers handle fallbacks.
	return conn, nil
}

func checkVecModule(ctx context.Context, conn *sql.Conn) (bool, error) {
	var one int
	if err := conn.QueryRowContext(ctx, "SELECT 1 FROM pragma_module_list WHERE name = 'vec'").Scan(&one); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		msg := err.Error()
		if strings.Contains(msg, "no such table: pragma_module_list") ||
			strings.Contains(msg, "no such table: pragma_module_list") ||
			strings.Contains(msg, "no such column: name") ||
			strings.Contains(msg, "syntax error") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func detectDriverFromDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return ""
	}
	lower := strings.ToLower(dsn)
	switch {
	case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
		return "postgres"
	case strings.HasPrefix(lower, "mysql://"):
		return "mysql"
	case strings.HasPrefix(lower, "file:"), lower == ":memory:", strings.HasSuffix(lower, ".sqlite"), strings.HasSuffix(lower, ".db"):
		return "sqlite"
	case strings.Contains(lower, "@tcp("), strings.Contains(lower, "@unix("):
		return "mysql"
	}
	return ""
}
