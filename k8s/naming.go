package k8s

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	dns1123MaxLength    = 63
	defaultResourceName = "resource"
)

// ComposeDNS1123Name builds a DNS-1123 compliant name from provided parts.
// It preserves readability when possible and appends a hash suffix when needed.
func ComposeDNS1123Name(parts ...string) string {
	baseParts := make([]string, 0, len(parts))
	for _, part := range parts {
		normalized := normalizeDNS1123Part(part)
		if normalized != "" {
			baseParts = append(baseParts, normalized)
		}
	}
	base := strings.Join(baseParts, "-")
	if base == "" {
		base = defaultResourceName
	}
	if len(base) <= dns1123MaxLength {
		return base
	}

	sum := sha1.Sum([]byte(base))
	suffix := hex.EncodeToString(sum[:])[:8]
	prefixLen := dns1123MaxLength - 1 - len(suffix)
	if prefixLen < 1 {
		if len(suffix) > dns1123MaxLength {
			return suffix[:dns1123MaxLength]
		}
		return suffix
	}

	prefix := strings.TrimSuffix(base[:prefixLen], "-")
	if prefix == "" {
		prefix = defaultResourceName
	}
	name := fmt.Sprintf("%s-%s", prefix, suffix)
	if len(name) > dns1123MaxLength {
		name = strings.TrimSuffix(name[:dns1123MaxLength], "-")
		if name == "" {
			return suffix
		}
	}
	return name
}

func normalizeDNS1123Part(part string) string {
	part = strings.ToLower(strings.TrimSpace(part))
	if part == "" {
		return ""
	}

	var builder strings.Builder
	lastDash := false
	for _, r := range part {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		default:
			if builder.Len() == 0 || lastDash {
				continue
			}
			builder.WriteByte('-')
			lastDash = true
		}
	}

	return strings.Trim(builder.String(), "-")
}
