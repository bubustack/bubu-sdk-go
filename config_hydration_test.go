package sdk

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bubustack/bobrapet/pkg/storage"
)

// mockHydratingStorageManager replaces $bubuStorageRef maps with the mapped
// replacement value, simulating what the real storage manager does.
type mockHydratingStorageManager struct {
	// replacements maps storage ref paths to the hydrated content.
	replacements map[string]any
}

func (m mockHydratingStorageManager) Hydrate(_ context.Context, data any) (any, error) {
	return m.hydrateValue(data), nil
}

func (m mockHydratingStorageManager) hydrateValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		if ref, ok := val[storage.StorageRefKey]; ok {
			if refStr, ok := ref.(string); ok {
				if replacement, ok := m.replacements[refStr]; ok {
					return replacement
				}
			}
			return val
		}
		result := make(map[string]any, len(val))
		for k, child := range val {
			result[k] = m.hydrateValue(child)
		}
		return result
	case []any:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = m.hydrateValue(item)
		}
		return result
	default:
		return v
	}
}

func (m mockHydratingStorageManager) Dehydrate(_ context.Context, data any, _ string) (any, error) {
	return data, nil
}

func TestHydrateConfig_SkipsTemplateEvalForStorageRefValues(t *testing.T) {
	// RSS body containing literal {{ account_id }} — must not be template-evaluated.
	rssBody := `<rss><item><title>Workers API: {{ account_id }} in path</title></item></rss>`

	sm := mockHydratingStorageManager{
		replacements: map[string]any{
			"outputs/step-1/body.json": rssBody,
		},
	}

	config := map[string]any{
		"model": "gpt-4o-mini",
		"userPrompt": map[string]any{
			storage.StorageRefKey: "outputs/step-1/body.json",
		},
		"temperature": 0,
	}

	celContext := map[string]any{}

	result, err := hydrateConfig(context.Background(), sm, config, celContext)
	if err != nil {
		t.Fatalf("hydrateConfig() returned unexpected error: %v", err)
	}

	// userPrompt should contain the raw RSS body, not a template-evaluation error.
	got, ok := result["userPrompt"]
	if !ok {
		t.Fatal("hydrateConfig() result missing 'userPrompt' key")
	}
	if got != rssBody {
		t.Errorf("userPrompt = %q, want %q", got, rssBody)
	}

	// Non-storage-ref values should still be present.
	if result["model"] != "gpt-4o-mini" {
		t.Errorf("model = %v, want 'gpt-4o-mini'", result["model"])
	}
}

func TestHydrateConfig_TemplateEvalStillWorksForNonStorageRefValues(t *testing.T) {
	sm := mockHydratingStorageManager{replacements: map[string]any{}}

	config := map[string]any{
		"greeting": "Hello {{ .inputs.name }}",
		"static":   "no templates here",
	}

	celContext := map[string]any{
		"inputs": map[string]any{"name": "world"},
	}

	result, err := hydrateConfig(context.Background(), sm, config, celContext)
	if err != nil {
		t.Fatalf("hydrateConfig() returned unexpected error: %v", err)
	}

	if result["greeting"] != "Hello world" {
		t.Errorf("greeting = %q, want 'Hello world'", result["greeting"])
	}
	if result["static"] != "no templates here" {
		t.Errorf("static = %q, want 'no templates here'", result["static"])
	}
}

func TestHydrateConfig_NestedStorageRefInArray(t *testing.T) {
	sm := mockHydratingStorageManager{
		replacements: map[string]any{
			"outputs/step-1/data.json": "content with {{ braces }}",
		},
	}

	config := map[string]any{
		"items": []any{
			map[string]any{
				storage.StorageRefKey: "outputs/step-1/data.json",
			},
		},
		"label": "safe value",
	}

	celContext := map[string]any{}

	result, err := hydrateConfig(context.Background(), sm, config, celContext)
	if err != nil {
		t.Fatalf("hydrateConfig() returned unexpected error: %v", err)
	}

	items, ok := result["items"].([]any)
	if !ok {
		t.Fatalf("items is %T, want []any", result["items"])
	}
	if len(items) != 1 || items[0] != "content with {{ braces }}" {
		t.Errorf("items = %v, want [\"content with {{ braces }}\"]", items)
	}
}

func TestHydrateConfig_NestedStorageRefDoesNotSuppressSiblingTemplates(t *testing.T) {
	rawBody := "raw body with {{ braces }}"
	sm := mockHydratingStorageManager{
		replacements: map[string]any{
			"outputs/step-1/body.txt": rawBody,
		},
	}

	config := map[string]any{
		"request": map[string]any{
			"body": map[string]any{
				storage.StorageRefKey: "outputs/step-1/body.txt",
			},
			"summary": "Summarize {{ .inputs.topic }}",
			"labels":  []any{"{{ .inputs.label }}"},
		},
	}

	celContext := map[string]any{
		"inputs": map[string]any{
			"topic": "release notes",
			"label": "urgent",
		},
	}

	result, err := hydrateConfig(context.Background(), sm, config, celContext)
	if err != nil {
		t.Fatalf("hydrateConfig() returned unexpected error: %v", err)
	}

	request, ok := result["request"].(map[string]any)
	if !ok {
		t.Fatalf("request is %T, want map[string]any", result["request"])
	}
	if request["body"] != rawBody {
		t.Fatalf("request.body = %q, want %q", request["body"], rawBody)
	}
	if request["summary"] != "Summarize release notes" {
		t.Fatalf("request.summary = %q, want %q", request["summary"], "Summarize release notes")
	}
	labels, ok := request["labels"].([]any)
	if !ok {
		t.Fatalf("request.labels is %T, want []any", request["labels"])
	}
	if len(labels) != 1 || labels[0] != "urgent" {
		t.Fatalf("request.labels = %v, want [urgent]", labels)
	}
}

func TestFindStorageRefPaths(t *testing.T) {
	config := map[string]any{
		"plain": "hello",
		"ref": map[string]any{
			storage.StorageRefKey: "outputs/step-1/data.json",
		},
		"nested": map[string]any{
			"inner": map[string]any{
				storage.StorageRefKey: "outputs/step-2/data.json",
			},
		},
		"list": []any{
			map[string]any{
				storage.StorageRefKey: "outputs/step-3/data.json",
			},
		},
		"number": 42,
	}

	paths := findStorageRefPaths(config)

	pathSet := make(map[string]bool, len(paths))
	for _, p := range paths {
		pathSet[configPathString(p)] = true
	}

	if !pathSet["ref"] {
		t.Error("expected 'ref' in storage ref paths")
	}
	if !pathSet["nested.inner"] {
		t.Error("expected 'nested.inner' in storage ref paths")
	}
	if !pathSet["list[0]"] {
		t.Error("expected 'list[0]' in storage ref paths")
	}
	if pathSet["plain"] {
		t.Error("'plain' should not be in storage ref paths")
	}
	if pathSet["number"] {
		t.Error("'number' should not be in storage ref paths")
	}
}

func TestExtractAndRestorePaths(t *testing.T) {
	m := map[string]any{
		"a": map[string]any{
			"inner": "value-a",
		},
		"b": []any{"value-b"},
		"c": "value-c",
	}

	extracted := extractPaths(m, []configPath{
		{{key: "a"}, {key: "inner"}},
		{{key: "b"}, {index: 0, isIndex: true}},
	})

	if len(extracted) != 2 {
		t.Fatalf("expected 2 extracted values, got %d", len(extracted))
	}
	if got := m["a"].(map[string]any)["inner"]; got == "value-a" {
		t.Fatalf("expected nested value to be replaced with placeholder, got %v", got)
	}
	if got := m["b"].([]any)[0]; got == "value-b" {
		t.Fatalf("expected array value to be replaced with placeholder, got %v", got)
	}

	restorePaths(m, extracted)

	if len(m) != 3 {
		t.Errorf("after restore, map has %d keys, want 3", len(m))
	}
	if got := m["a"].(map[string]any)["inner"]; got != "value-a" {
		t.Errorf("restored a.inner = %v, want value-a", got)
	}
	if got := m["b"].([]any)[0]; got != "value-b" {
		t.Errorf("restored b[0] = %v, want value-b", got)
	}
	if m["c"] != "value-c" {
		t.Errorf("restored map = %v", m)
	}
}

func TestHydrateConfigRejectsCyclicConfig(t *testing.T) {
	config := map[string]any{}
	config["self"] = config

	_, err := hydrateConfig(context.Background(), mockHydratingStorageManager{}, config, nil)
	if err == nil {
		t.Fatal("expected cyclic config to be rejected")
	}
	if got := err.Error(); got == "" || !containsAll(got, "config rejected before hydration", "cycle") {
		t.Fatalf("expected cycle rejection error, got %v", err)
	}
}

func TestHydrateConfigRejectsExcessiveDepth(t *testing.T) {
	t.Setenv(configHydrationMaxDepthEnv, "2")

	config := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": "too deep",
			},
		},
	}

	_, err := hydrateConfig(context.Background(), mockHydratingStorageManager{}, config, nil)
	if err == nil {
		t.Fatal("expected deep config to be rejected")
	}
	if got := err.Error(); got == "" || !containsAll(got, "config rejected before hydration", "max depth 2") {
		t.Fatalf("expected depth rejection error, got %v", err)
	}
}

func TestHydrateConfigRejectsHydratedConfigThatExceedsNodeBudget(t *testing.T) {
	t.Setenv(configHydrationMaxNodesEnv, "3")

	sm := mockHydratingStorageManager{
		replacements: map[string]any{
			"outputs/step-1/payload.json": map[string]any{
				"a": map[string]any{
					"b": "c",
				},
			},
		},
	}

	config := map[string]any{
		"payload": map[string]any{
			storage.StorageRefKey: "outputs/step-1/payload.json",
		},
	}

	_, err := hydrateConfig(context.Background(), sm, config, nil)
	if err == nil {
		t.Fatal("expected hydrated config node budget rejection")
	}
	if got := err.Error(); got == "" || !containsAll(got, "hydrated config rejected before template resolution",
		"max nodes 3") {
		t.Fatalf("expected hydrated node budget rejection, got %v", err)
	}
}

func containsAll(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if !strings.Contains(s, substr) {
			return false
		}
	}
	return true
}

func configPathString(path configPath) string {
	if len(path) == 0 {
		return ""
	}
	var out strings.Builder
	for i, segment := range path {
		if segment.isIndex {
			out.WriteString(fmt.Sprintf("[%d]", segment.index))
			continue
		}
		if i > 0 {
			out.WriteString(".")
		}
		out.WriteString(segment.key)
	}
	return out.String()
}
