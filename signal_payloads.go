package sdk

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"strings"
	"unicode/utf8"
)

const (
	defaultSignalSampleBytes = 2048
)

// SignalMeta describes a lightweight summary of a payload that is safe for signals.
type SignalMeta struct {
	// Format identifies the logical payload shape, such as "text" or "json".
	Format string `json:"format,omitempty"`
	// ContentType carries the MIME content type when one is known.
	ContentType string `json:"contentType,omitempty"`
	// SizeBytes reports the original payload size in bytes.
	SizeBytes int `json:"sizeBytes,omitempty"`
	// HashSHA256 optionally carries the lowercase hex SHA-256 digest of the original payload.
	HashSHA256 string `json:"hashSha256,omitempty"`
	// Attributes carries additional safe, non-secret metadata about the payload.
	Attributes map[string]string `json:"attributes,omitempty"`
}

// SignalEnvelope is the standard signal structure for metadata + optional samples.
type SignalEnvelope struct {
	// Meta summarizes the full payload without embedding it directly in StepRun status.
	Meta SignalMeta `json:"meta,omitempty"`
	// Sample carries an optional, already-sanitized sample payload when callers choose to include one.
	Sample any `json:"sample,omitempty"`
}

// TextSignalOptions controls how text payloads are summarized for signals.
type TextSignalOptions struct {
	// Format overrides the logical payload format label stored in SignalMeta.Format.
	Format string
	// ContentType sets SignalMeta.ContentType for the emitted signal.
	ContentType string
	// SampleBytes caps the inline sample size in bytes when callers choose to include a text sample.
	// When zero, EmitTextSignal preserves the default metadata-only behavior unless SampleExtras is set.
	SampleBytes int
	// IncludeHash enables SHA-256 hashing of the original text into SignalMeta.HashSHA256.
	IncludeHash bool
	// Attributes attaches additional safe metadata to SignalMeta.Attributes.
	Attributes map[string]string
	// SampleExtras adds additional sample metadata alongside the sampled text.
	SampleExtras map[string]any
}

// EmitTextSignal emits a metadata summary for text payloads. By default it keeps
// metadata-only behavior; callers can opt into bounded inline samples via
// SampleBytes or SampleExtras. The full payload should still be passed via
// outputs/storage references when downstream steps need the complete content.
func EmitTextSignal(ctx context.Context, key string, text string, opts TextSignalOptions) error {
	sizeBytes := len(text)
	meta := SignalMeta{
		Format:      firstNonEmpty(opts.Format, "text"),
		ContentType: strings.TrimSpace(opts.ContentType),
		SizeBytes:   sizeBytes,
		Attributes:  opts.Attributes,
	}
	if opts.IncludeHash {
		sum := sha256.Sum256([]byte(text))
		meta.HashSHA256 = fmt.Sprintf("%x", sum)
	}
	payload := SignalEnvelope{
		Meta:   meta,
		Sample: buildTextSignalSample(text, opts),
	}
	return EmitSignal(ctx, key, payload)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func mergeSample(base map[string]any, extras map[string]any) map[string]any {
	if len(extras) == 0 {
		return base
	}
	out := make(map[string]any, len(base)+len(extras))
	maps.Copy(out, base)
	for key, value := range extras {
		if _, exists := out[key]; !exists {
			out[key] = value
		}
	}
	return out
}

func buildTextSignalSample(text string, opts TextSignalOptions) any {
	if opts.SampleBytes == 0 && len(opts.SampleExtras) == 0 {
		return nil
	}
	limit := opts.SampleBytes
	if limit <= 0 {
		limit = defaultSignalSampleBytes
	}
	sampleText, truncated := truncateUTF8StringBytes(text, limit)
	sample := map[string]any{
		"text": sampleText,
	}
	if truncated {
		sample["truncated"] = true
	}
	return mergeSample(sample, opts.SampleExtras)
}

func truncateUTF8StringBytes(text string, maxBytes int) (string, bool) {
	if maxBytes <= 0 || len(text) <= maxBytes {
		return text, false
	}
	truncated := text[:maxBytes]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated, true
}
