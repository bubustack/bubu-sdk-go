// Package media provides helpers for offloading large streaming payloads to
// shared object storage while keeping small payloads inline.
package media

import (
	"context"
	crypto_rand "crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/core/contracts"
)

const defaultInlineLimit = 48 * 1024 // 48 KiB keeps short bursts inline for real-time UX

// StorageReference describes an object persisted to the shared storage backend.
type StorageReference struct {
	// Path is the object path inside the configured shared storage backend.
	Path string `json:"path"`
	// ContentType is the MIME type recorded for the stored object.
	ContentType string `json:"contentType,omitempty"`
	// SizeBytes is the stored blob size when known.
	SizeBytes int `json:"sizeBytes,omitempty"`
}

// WriteOptions control how object paths are composed when offloading payloads.
type WriteOptions struct {
	// Namespace scopes the object path to the owning runtime namespace.
	Namespace string
	// StoryRun scopes the object path to the owning StoryRun.
	StoryRun string
	// Step scopes the object path to the owning step.
	Step string
	// Scope appends additional caller-defined path segments.
	Scope []string
	// ContentType records the MIME type written for the object.
	ContentType string
	// InlineLimit overrides the default inline/offload threshold.
	InlineLimit int
}

// MaybeOffloadBlob stores the provided data if it exceeds the inline limit.
// Returns a storage reference when offload happens; otherwise nil.
func MaybeOffloadBlob(
	ctx context.Context,
	sm *storage.StorageManager,
	data []byte,
	opts WriteOptions,
) (*StorageReference, error) {
	if sm == nil || sm.GetStore() == nil || len(data) == 0 {
		return nil, nil
	}
	limit := inlineLimit(opts.InlineLimit)
	if len(data) <= limit {
		return nil, nil
	}

	contentType := firstNonEmpty(opts.ContentType, "application/octet-stream")
	objectPath := buildPath(opts, time.Now().UTC())
	if err := sm.WriteBlob(ctx, objectPath, contentType, data); err != nil {
		return nil, err
	}

	return &StorageReference{
		Path:        objectPath,
		ContentType: contentType,
		SizeBytes:   len(data),
	}, nil
}

// ReadBlob loads raw bytes for the given reference.
func ReadBlob(ctx context.Context, sm *storage.StorageManager, ref *StorageReference) ([]byte, error) {
	if ref == nil || strings.TrimSpace(ref.Path) == "" {
		return nil, fmt.Errorf("storage reference missing path")
	}
	if sm == nil || sm.GetStore() == nil {
		return nil, fmt.Errorf("storage is disabled")
	}
	return sm.ReadBlob(ctx, ref.Path)
}

func inlineLimit(override int) int {
	if override > 0 {
		return override
	}
	if v := os.Getenv(contracts.MediaInlineSizeEnv); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	if v := os.Getenv(contracts.MaxInlineSizeEnv); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return defaultInlineLimit
}

func buildPath(opts WriteOptions, ts time.Time) string {
	segments := []string{"streams"}
	for _, seg := range []string{opts.Namespace, opts.StoryRun, opts.Step} {
		if sanitized := sanitizeSegment(seg); sanitized != "" {
			segments = append(segments, sanitized)
		}
	}
	for _, scope := range opts.Scope {
		if sanitized := sanitizeSegment(scope); sanitized != "" {
			segments = append(segments, sanitized)
		}
	}
	segments = append(segments, ts.Format("2006/01/02"))
	filename := fmt.Sprintf("%d-%06d-%s.bin", ts.Unix(), ts.Nanosecond()/1000, randHexSuffix(4))
	segments = append(segments, filename)
	return path.Join(segments...)
}

var sanitizeRe = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func sanitizeSegment(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	cleaned := sanitizeRe.ReplaceAllString(trimmed, "-")
	cleaned = strings.Trim(cleaned, "-_.")
	if cleaned == "" {
		return ""
	}
	if len(cleaned) > 64 {
		cleaned = cleaned[:64]
	}
	return strings.ToLower(cleaned)
}

func randHexSuffix(n int) string {
	b := make([]byte, n)
	_, _ = crypto_rand.Read(b)
	return hex.EncodeToString(b)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
