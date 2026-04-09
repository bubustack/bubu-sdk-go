package sdk

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadAllowedLogFileRejectsPathOutsideConfiguredRoots(t *testing.T) {
	allowedDir := t.TempDir()
	disallowedDir := t.TempDir()
	path := filepath.Join(disallowedDir, "app.log")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	t.Setenv(logAllowedRootsEnv, allowedDir)

	_, err := readAllowedLogFile(path)
	if err == nil {
		t.Fatal("expected path outside configured roots to fail")
	}
	if !strings.Contains(err.Error(), "outside allowed roots") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadAllowedLogFileRejectsOversizedFiles(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "large.log")
	if err := os.WriteFile(path, []byte("abcdef"), 0o600); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	t.Setenv(logAllowedRootsEnv, root)
	t.Setenv(logFileMaxBytesEnv, "4")

	_, err := readAllowedLogFile(path)
	if err == nil {
		t.Fatal("expected oversized log file to fail")
	}
	if !strings.Contains(err.Error(), "exceeds max bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadAllowedLogFileAllowsConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "app.log")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	t.Setenv(logAllowedRootsEnv, root)

	data, err := readAllowedLogFile(path)
	if err != nil {
		t.Fatalf("readAllowedLogFile returned error: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected content: %q", string(data))
	}
}
