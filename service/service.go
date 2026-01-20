package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

// WithEmbedder sets the default embedder.
func WithEmbedder(embedder embeddings.Embedder) Option {
	return func(s *Service) { s.embedder = embedder }
}

// Service exposes reusable operations for indexing, search, sync, and admin.
type Service struct {
	db       *sql.DB
	dsn      string
	embedder embeddings.Embedder
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
	if s.db != nil {
		if err := vec.Register(s.db); err != nil {
			return nil, err
		}
		if withAdmin {
			if err := vecadmin.Register(s.db); err != nil {
				return nil, err
			}
		}
		return s.db, nil
	}
	if dsn == "" {
		dsn = s.dsn
	}
	if dsn == "" {
		return nil, fmt.Errorf("sqlite dsn required")
	}
	bd, err := engine.Open(dsn)
	if err != nil {
		return nil, err
	}
	bd.SetMaxOpenConns(4)
	bd.SetMaxIdleConns(4)
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

func ensureSchemaConn(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if err := ensureSchema(ctx, conn); err != nil {
		if strings.Contains(err.Error(), "no such module: vec") {
			_ = conn.Close()
			if err := vec.Register(db); err != nil {
				return nil, err
			}
			conn, err = db.Conn(ctx)
			if err != nil {
				return nil, err
			}
			if err := ensureSchema(ctx, conn); err != nil {
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
