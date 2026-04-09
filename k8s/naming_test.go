package k8s

import (
	"regexp"
	"strings"
	"testing"
)

var dns1123LabelPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func TestComposeDNS1123NameNormalizesInvalidCharacters(t *testing.T) {
	got := ComposeDNS1123Name("My_App", "UPPER", "--bad--")
	want := "my-app-upper-bad"
	if got != want {
		t.Fatalf("ComposeDNS1123Name() = %q, want %q", got, want)
	}
}

func TestComposeDNS1123NameFallsBackWhenPartsNormalizeEmpty(t *testing.T) {
	if got := ComposeDNS1123Name("!!!", "---"); got != "resource" { //nolint:goconst
		t.Fatalf("ComposeDNS1123Name() = %q, want %q", got, "resource")
	}
}

func TestComposeDNS1123NameLongNameRemainsCompliant(t *testing.T) {
	got := ComposeDNS1123Name(strings.Repeat("A", 80), "Part_With_Invalid_Chars")
	if len(got) > dns1123MaxLength {
		t.Fatalf("name length = %d, want <= %d", len(got), dns1123MaxLength)
	}
	if !dns1123LabelPattern.MatchString(got) {
		t.Fatalf("generated name %q is not DNS-1123 compliant", got)
	}
}
