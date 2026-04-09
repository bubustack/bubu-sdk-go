package media

import (
	"strings"
	"testing"
	"time"
)

func TestSanitizeSegment(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"whitespace only", "   ", ""},
		{"simple", "hello", "hello"},
		{"uppercase", "Hello", "hello"},
		{"special chars", "foo/bar:baz", "foo-bar-baz"},
		{"unicode", "cafe\u0301", "cafe"},
		{"leading special", "---foo", "foo"},
		{"trailing special", "foo---", "foo"},
		{"all special", "@#$%", ""},
		{"dots and dashes", "a.b-c_d", "a.b-c_d"},
		{"long string truncated", strings.Repeat("a", 100), strings.Repeat("a", 64)},
		{"mixed", "  My Story Run! ", "my-story-run"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeSegment(tt.in)
			if got != tt.want {
				t.Errorf("sanitizeSegment(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestBuildPath(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 500000000, time.UTC)

	tests := []struct {
		name     string
		opts     WriteOptions
		contains []string
	}{
		{
			name: "full scope",
			opts: WriteOptions{Namespace: "ns1", StoryRun: "sr1", Step: "step1"},
			contains: []string{
				"streams/", "ns1/", "sr1/", "step1/", "2025/06/15/",
			},
		},
		{
			name: "empty scope",
			opts: WriteOptions{},
			contains: []string{
				"streams/", "2025/06/15/",
			},
		},
		{
			name: "with extra scope",
			opts: WriteOptions{Scope: []string{"transport-a", "partition-0"}},
			contains: []string{
				"streams/", "transport-a/", "partition-0/", "2025/06/15/",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := buildPath(tt.opts, ts)
			for _, sub := range tt.contains {
				if !strings.Contains(p, sub) {
					t.Errorf("buildPath() = %q, missing %q", p, sub)
				}
			}
			if strings.Contains(p, "\\") {
				t.Errorf("buildPath() = %q, contains backslash", p)
			}
			if !strings.HasSuffix(p, ".bin") {
				t.Errorf("buildPath() = %q, should end with .bin", p)
			}
		})
	}
}

func TestBuildPathUniqueness(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 500000000, time.UTC)
	opts := WriteOptions{Namespace: "ns", StoryRun: "sr", Step: "s"}

	seen := make(map[string]bool)
	for i := range 100 {
		p := buildPath(opts, ts)
		if seen[p] {
			t.Fatalf("collision on iteration %d: %q", i, p)
		}
		seen[p] = true
	}
}

func TestRandHexSuffix(t *testing.T) {
	s := randHexSuffix(4)
	if len(s) != 8 {
		t.Errorf("randHexSuffix(4) length = %d, want 8", len(s))
	}
	s2 := randHexSuffix(4)
	if s == s2 {
		t.Errorf("two calls returned same value: %q", s)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{"first", []string{"a", "b"}, "a"},
		{"skip empty", []string{"", "b"}, "b"},
		{"skip whitespace", []string{"  ", "b"}, "b"},
		{"all empty", []string{"", ""}, ""},
		{"none", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := firstNonEmpty(tt.values...)
			if got != tt.want {
				t.Errorf("firstNonEmpty(%v) = %q, want %q", tt.values, got, tt.want)
			}
		})
	}
}

func TestInlineLimit(t *testing.T) {
	tests := []struct {
		name     string
		override int
		want     int
	}{
		{"default", 0, defaultInlineLimit},
		{"override", 1024, 1024},
		{"negative ignored", -1, defaultInlineLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inlineLimit(tt.override)
			if got != tt.want {
				t.Errorf("inlineLimit(%d) = %d, want %d", tt.override, got, tt.want)
			}
		})
	}
}
