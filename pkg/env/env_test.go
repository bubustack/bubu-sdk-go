package env

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestGetDuration(t *testing.T) {
	const key = "TEST_DURATION_ENV"
	defaultValue := 5 * time.Second

	t.Run("unset uses default", func(t *testing.T) {
		t.Setenv(key, "")
		if got := GetDuration(key, defaultValue); got != defaultValue {
			t.Fatalf("GetDuration() = %s, want %s", got, defaultValue)
		}
	})

	t.Run("valid duration is parsed", func(t *testing.T) {
		t.Setenv(key, "150ms")
		if got := GetDuration(key, defaultValue); got != 150*time.Millisecond {
			t.Fatalf("GetDuration() = %s, want 150ms", got)
		}
	})

	t.Run("invalid duration warns and falls back", func(t *testing.T) {
		t.Setenv(key, "not-a-duration")

		var buf bytes.Buffer
		prev := slog.Default()
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		slog.SetDefault(logger)
		defer slog.SetDefault(prev)

		if got := GetDuration(key, defaultValue); got != defaultValue {
			t.Fatalf("GetDuration() = %s, want default %s", got, defaultValue)
		}
		if out := buf.String(); out == "" || !strings.Contains(out, key) {
			t.Fatalf("expected warning log for invalid duration, got %q", out)
		}
	})
}

func TestGetInt(t *testing.T) {
	const key = "TEST_INT_ENV"
	defaultValue := 5

	t.Run("unset uses default", func(t *testing.T) {
		t.Setenv(key, "")
		if got := GetInt(key, defaultValue); got != defaultValue {
			t.Fatalf("GetInt() = %d, want %d", got, defaultValue)
		}
	})

	t.Run("valid int is parsed", func(t *testing.T) {
		t.Setenv(key, "42")
		if got := GetInt(key, defaultValue); got != 42 {
			t.Fatalf("GetInt() = %d, want 42", got)
		}
	})

	t.Run("invalid int warns and falls back", func(t *testing.T) {
		t.Setenv(key, "-1")

		var buf bytes.Buffer
		prev := slog.Default()
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		slog.SetDefault(logger)
		defer slog.SetDefault(prev)

		if got := GetInt(key, defaultValue); got != defaultValue {
			t.Fatalf("GetInt() = %d, want default %d", got, defaultValue)
		}
		if out := buf.String(); out == "" || !strings.Contains(out, key) {
			t.Fatalf("expected warning log for invalid int, got %q", out)
		}
	})
}
