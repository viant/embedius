package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config defines root mappings for batch operations.
type Config struct {
	DB    string                `yaml:"db"`
	Roots map[string]RootConfig `yaml:"roots"`
}

// RootConfig defines per-root settings.
type RootConfig struct {
	Path         string   `yaml:"path"`
	Include      []string `yaml:"include"`
	Exclude      []string `yaml:"exclude"`
	MaxSizeBytes int64    `yaml:"max_size_bytes"`
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
