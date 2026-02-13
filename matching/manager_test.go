package matching

import (
	"strings"
	"testing"

	"github.com/viant/embedius/matching/option"
)

func TestManager_IsExcluded_Table(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		size     int
		options  []option.Option
		excluded bool
	}{
		{
			name:     "url scheme excluded by suffix glob",
			path:     "s3://bucket/dir/foo_test.go",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns("**/*_test.go")},
			excluded: true,
		},
		{
			name:     "url scheme included by suffix glob",
			path:     "s3://bucket/dir/foo.go",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns("**/*_test.go")},
			excluded: false,
		},
		{
			name: "include go then exclude tests",
			path: "s3://bucket/dir/foo.go",
			size: 1,
			options: []option.Option{
				option.WithInclusionPatterns("**/*.go"),
				option.WithExclusionPatterns("**/*_test.go"),
			},
			excluded: false,
		},
		{
			name: "include go then exclude tests (test file)",
			path: "s3://bucket/dir/foo_test.go",
			size: 1,
			options: []option.Option{
				option.WithInclusionPatterns("**/*.go"),
				option.WithExclusionPatterns("**/*_test.go"),
			},
			excluded: true,
		},
		{
			name: "include go excludes non-go",
			path: "s3://bucket/dir/readme.txt",
			size: 1,
			options: []option.Option{
				option.WithInclusionPatterns("**/*.go"),
			},
			excluded: true,
		},
		{
			name:     "directory pattern with slash",
			path:     "s3://bucket/app/node_modules/pkg/index.js",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns("node_modules/")},
			excluded: true,
		},
		{
			name:     "directory pattern does not match sibling",
			path:     "s3://bucket/app/modules/node_modules.js",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns("node_modules/")},
			excluded: false,
		},
		{
			name:     "dir glob with /** matches nested",
			path:     "s3://bucket/app/vendor/github.com/mod/file.go",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns("**/vendor/**")},
			excluded: true,
		},
		{
			name:     "basename wildcard matches",
			path:     "/tmp/a/b/c/file.min.js",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns("*.min.js")},
			excluded: true,
		},
		{
			name:     "basename exact matches",
			path:     "/tmp/a/b/.DS_Store",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns(".DS_Store")},
			excluded: true,
		},
		{
			name:     "max size excludes",
			path:     "/tmp/a/b/big.bin",
			size:     101,
			options:  []option.Option{option.WithMaxIndexableSize(100)},
			excluded: true,
		},
		{
			name:     "max size allows smaller",
			path:     "/tmp/a/b/small.bin",
			size:     99,
			options:  []option.Option{option.WithMaxIndexableSize(100)},
			excluded: false,
		},
		{
			name:     "leading **/ matches basename",
			path:     "gs://bkt/dir/note.txt",
			size:     1,
			options:  []option.Option{option.WithExclusionPatterns("**/note.txt")},
			excluded: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(tt.options...)
			if got := m.IsExcluded(tt.path, tt.size); got != tt.excluded {
				t.Fatalf("IsExcluded(%q)=%v want %v", tt.path, got, tt.excluded)
			}
		})
	}
}

func TestManager_IsExcluded_WithGitignore(t *testing.T) {
	gitignore := strings.NewReader(`
# comment
*.log
dist/
**/*_test.go
!keep.log
/build
tmp/
docs/*.md
**/cache/**

`)
	m := New(option.WithGitignore(gitignore))

	cases := []struct {
		path     string
		excluded bool
	}{
		{path: "s3://bucket/app/debug.log", excluded: true},
		{path: "s3://bucket/app/keep.log", excluded: false},
		{path: "s3://bucket/app/dist/main.js", excluded: true},
		{path: "s3://bucket/app/foo_test.go", excluded: true},
		{path: "s3://bucket/app/main.go", excluded: false},
		{path: "s3://bucket/build/app.js", excluded: true},
		{path: "s3://bucket/dir/build/app.js", excluded: false},
		{path: "s3://bucket/tmp/file.txt", excluded: true},
		{path: "s3://bucket/dir/tmp/file.txt", excluded: true},
		{path: "s3://bucket/docs/readme.md", excluded: true},
		{path: "s3://bucket/dir/docs/readme.md", excluded: false},
		{path: "s3://bucket/dir/cache/file.bin", excluded: true},
	}

	for _, tc := range cases {
		if got := m.IsExcluded(tc.path, 1); got != tc.excluded {
			t.Fatalf("IsExcluded(%q)=%v want %v", tc.path, got, tc.excluded)
		}
	}
}

func TestManager_IsExcluded_DirectorySemantics(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		path     string
		excluded bool
	}{
		{
			name:     "**/dir/** matches nested dir",
			pattern:  "**/dir/**",
			path:     "s3://bucket/app/dir/sub/file.go",
			excluded: true,
		},
		{
			name:     "dir/** matches root dir",
			pattern:  "dir/**",
			path:     "dir/sub/file.go",
			excluded: true,
		},
		{
			name:     "dir/** matches nested dir",
			pattern:  "dir/**",
			path:     "s3://bucket/app/dir/sub/file.go",
			excluded: true,
		},
		{
			name:     "dir/ matches only directory segment",
			pattern:  "dir/",
			path:     "s3://bucket/app/dir/file.go",
			excluded: true,
		},
		{
			name:     "dir/ does not match substring",
			pattern:  "dir/",
			path:     "s3://bucket/app/adir/file.go",
			excluded: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(option.WithExclusionPatterns(tt.pattern))
			if got := m.IsExcluded(tt.path, 1); got != tt.excluded {
				t.Fatalf("IsExcluded(%q)=%v want %v", tt.path, got, tt.excluded)
			}
		})
	}
}

func TestManager_IsExcluded_IncludeExcludeOverlap(t *testing.T) {
	m := New(
		option.WithInclusionPatterns("**/*.go", "**/gen/**/*.go"),
		option.WithExclusionPatterns("**/gen/**", "**/*_test.go"),
	)

	cases := []struct {
		path     string
		excluded bool
	}{
		{path: "s3://bucket/app/main.go", excluded: false},
		{path: "s3://bucket/app/main_test.go", excluded: true},
		{path: "s3://bucket/app/gen/keep.go", excluded: true},
		{path: "s3://bucket/app/gen/keep_test.go", excluded: true},
	}

	for _, tc := range cases {
		if got := m.IsExcluded(tc.path, 1); got != tc.excluded {
			t.Fatalf("IsExcluded(%q)=%v want %v", tc.path, got, tc.excluded)
		}
	}
}

func TestManager_IsExcluded_WindowsPaths(t *testing.T) {
	m := New(option.WithExclusionPatterns("**/*_test.go"))

	cases := []struct {
		path     string
		excluded bool
	}{
		{path: `C:\repo\dir\file_test.go`, excluded: true},
		{path: `C:\repo\dir\file.go`, excluded: false},
		{path: `\\server\share\dir\file_test.go`, excluded: true},
	}

	for _, tc := range cases {
		if got := m.IsExcluded(tc.path, 1); got != tc.excluded {
			t.Fatalf("IsExcluded(%q)=%v want %v", tc.path, got, tc.excluded)
		}
	}
}
