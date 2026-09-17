// Package metadata extracts source-format metadata for indexed documents.
package metadata

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config declares a generic document metadata extractor. Fields maps an
// indexed metadata key to a source key in the extracted document header.
// Example: document.title: title maps frontmatter's title field to the neutral
// document.title metadata key.
type Config struct {
	Extractor string            `yaml:"extractor,omitempty" json:"extractor,omitempty"`
	Fields    map[string]string `yaml:"fields,omitempty" json:"fields,omitempty"`
}

// StringValues converts extracted scalar metadata to the fragment metadata
// representation used by the filesystem indexer.
func StringValues(values map[string]any) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		switch actual := value.(type) {
		case string:
			result[key] = actual
		case bool:
			result[key] = strconv.FormatBool(actual)
		case int:
			result[key] = strconv.Itoa(actual)
		case int64:
			result[key] = strconv.FormatInt(actual, 10)
		case float64:
			result[key] = strconv.FormatFloat(actual, 'f', -1, 64)
		default:
			result[key] = fmt.Sprint(actual)
		}
	}
	return result
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.Extractor) != "" && len(c.Fields) > 0
}

// Targets returns the indexed metadata keys managed by this configuration.
func (c Config) Targets() []string {
	result := make([]string, 0, len(c.Fields))
	for target := range c.Fields {
		target = strings.TrimSpace(target)
		if target != "" {
			result = append(result, target)
		}
	}
	return result
}

// Extract parses configured source metadata and returns the configured,
// normalized keys. It intentionally knows only source formats, never vendors
// or domain vocabulary.
func (c Config) Extract(data []byte) (map[string]any, error) {
	if !c.Enabled() {
		return nil, nil
	}
	var source map[string]any
	switch strings.ToLower(strings.TrimSpace(c.Extractor)) {
	case "yaml-frontmatter", "yaml_frontmatter":
		var err error
		source, err = yamlFrontmatter(data)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported metadata extractor %q", c.Extractor)
	}
	out := make(map[string]any, len(c.Fields))
	for target, key := range c.Fields {
		target = strings.TrimSpace(target)
		key = strings.TrimSpace(key)
		if target == "" || key == "" {
			continue
		}
		if value, ok := source[key]; ok && value != nil {
			out[target] = value
		}
	}
	return out, nil
}

func yamlFrontmatter(data []byte) (map[string]any, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	if !bytes.HasPrefix(data, []byte("---\n")) && !bytes.Equal(bytes.TrimSpace(data), []byte("---")) {
		return map[string]any{}, nil
	}
	lines := bytes.Split(data, []byte("\n"))
	end := -1
	for i := 1; i < len(lines); i++ {
		marker := bytes.TrimSpace(lines[i])
		if bytes.Equal(marker, []byte("---")) || bytes.Equal(marker, []byte("...")) {
			end = i
			break
		}
	}
	if end < 0 {
		return nil, fmt.Errorf("yaml frontmatter is missing a closing delimiter")
	}
	result := map[string]any{}
	if err := yaml.Unmarshal(bytes.Join(lines[1:end], []byte("\n")), &result); err != nil {
		return nil, fmt.Errorf("decode yaml frontmatter: %w", err)
	}
	return result, nil
}
