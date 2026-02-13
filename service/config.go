package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/scy/cred/secret"
	"gopkg.in/yaml.v3"
)

// Config defines root mappings for batch operations.
type Config struct {
	DB        string                `yaml:"db"`
	Store     StoreConfig           `yaml:"store"`
	Roots     map[string]RootConfig `yaml:"roots"`
	Upstreams []UpstreamConfig      `yaml:"upstreams"`
	MCPServer MCPServerConfig       `yaml:"mcpServer"`
}

// StoreConfig defines vector store settings.
type StoreConfig struct {
	DSN    string `yaml:"dsn"`
	Driver string `yaml:"driver"`
	Secret string `yaml:"secret,omitempty"`
}

// RootConfig defines per-root settings.
type RootConfig struct {
	Path         string   `yaml:"path"`
	Include      []string `yaml:"include"`
	Exclude      []string `yaml:"exclude"`
	MaxSizeBytes int64    `yaml:"max_size_bytes"`
	UpstreamRef  string   `yaml:"upstreamRef"`
}

// UpstreamConfig defines upstream sync settings.
type UpstreamConfig struct {
	Name               string `yaml:"name"`
	Driver             string `yaml:"driver"`
	DSN                string `yaml:"dsn"`
	Secret             string `yaml:"secret,omitempty"`
	Shadow             string `yaml:"shadow"`
	Batch              int    `yaml:"batch"`
	Force              bool   `yaml:"force"`
	Enabled            bool   `yaml:"enabled"`
	MinIntervalSeconds int    `yaml:"minIntervalSeconds"`
}

// MCPServerConfig defines MCP server settings.
type MCPServerConfig struct {
	Addr string `yaml:"addr"`
	Port int    `yaml:"port"`
}

func LoadConfig(path string) (*Config, error) {
	path, err := expandUserPath(path)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	// Backward compatible: roots as map[string]string.
	if len(cfg.Roots) == 0 {
		var raw struct {
			DB    string            `yaml:"db"`
			Roots map[string]string `yaml:"roots"`
		}
		if err := yaml.Unmarshal(b, &raw); err == nil && len(raw.Roots) > 0 {
			cfg.DB = raw.DB
			cfg.Roots = map[string]RootConfig{}
			for name, p := range raw.Roots {
				cfg.Roots[name] = RootConfig{Path: p}
			}
		}
	}
	if cfg.DB != "" {
		if expanded, err := expandUserPath(cfg.DB); err == nil {
			cfg.DB = expanded
		} else {
			return nil, err
		}
	}
	if cfg.Store.DSN != "" {
		if expanded, err := expandStoreDSN(cfg.Store.DSN, cfg.Store.Driver); err == nil {
			cfg.Store.DSN = expanded
		} else {
			return nil, err
		}
	}
	if cfg.Store.Secret != "" {
		if expanded, err := ExpandDSNWithSecret(context.Background(), cfg.Store.DSN, cfg.Store.Secret); err == nil {
			cfg.Store.DSN = expanded
		} else {
			return nil, err
		}
	}
	for name, root := range cfg.Roots {
		if root.Path == "" {
			continue
		}
		expanded, err := expandUserPath(root.Path)
		if err != nil {
			return nil, err
		}
		root.Path = expanded
		cfg.Roots[name] = root
	}
	for i, up := range cfg.Upstreams {
		if strings.TrimSpace(up.Secret) == "" {
			continue
		}
		expanded, err := ExpandDSNWithSecret(context.Background(), up.DSN, up.Secret)
		if err != nil {
			return nil, err
		}
		up.DSN = expanded
		cfg.Upstreams[i] = up
	}
	return &cfg, nil
}

func expandUserPath(path string) (string, error) {
	if path == "" || path[0] != '~' {
		return path, nil
	}
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return "", fmt.Errorf("config: unsupported ~user path: %s", path)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if path == "~" {
		return home, nil
	}
	return filepath.Join(home, path[2:]), nil
}

func expandStoreDSN(dsn, driver string) (string, error) {
	if dsn == "" {
		return dsn, nil
	}
	// Expand user path only for sqlite-like DSNs or plain paths.
	if driver == "sqlite" || dsn[0] == '~' || dsn[0] == '/' || strings.HasPrefix(dsn, "file:") {
		return expandUserPath(dsn)
	}
	return dsn, nil
}

// ExpandDSNWithSecret loads a secret and expands placeholders in the DSN.
func ExpandDSNWithSecret(ctx context.Context, dsn, secretRef string) (string, error) {
	secretRef = strings.TrimSpace(secretRef)
	if secretRef == "" {
		return dsn, nil
	}
	if strings.TrimSpace(dsn) == "" {
		return "", fmt.Errorf("secret %q provided but dsn is empty", secretRef)
	}
	svc := secret.New()
	sec, err := svc.Lookup(ctx, secret.Resource(secretRef))
	if err != nil {
		return "", err
	}
	return sec.Expand(dsn), nil
}
