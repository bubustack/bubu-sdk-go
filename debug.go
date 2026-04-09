package sdk

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/core/contracts"
)

const debugPreviewLimit = 2048

var truthyDebugValues = map[string]struct{}{
	"1":     {},
	"true":  {},
	"t":     {},
	"yes":   {},
	"y":     {},
	"on":    {},
	"debug": {},
}

func isDebugEnabled() bool {
	raw := strings.TrimSpace(os.Getenv(contracts.DebugEnv))
	if raw == "" {
		return false
	}
	_, ok := truthyDebugValues[strings.ToLower(raw)]
	return ok
}

// DebugModeEnabled reports whether verbose logging should be forced regardless of logger level.
func DebugModeEnabled() bool {
	return isDebugEnabled()
}

func newDefaultLogger() *slog.Logger {
	opts := &slog.HandlerOptions{}
	if isDebugEnabled() {
		opts.Level = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func logExecutionContextDebug(logger *slog.Logger, data *runtime.ExecutionContextData) {
	if !isDebugEnabled() || logger == nil || data == nil {
		return
	}

	attrs := []slog.Attr{
		slog.String("story", data.StoryInfo.StoryName),
		slog.String("storyRun", data.StoryInfo.StoryRunID),
		slog.String("step", data.StoryInfo.StepName),
		slog.String("stepRun", data.StoryInfo.StepRunID),
		slog.String("namespace", data.StoryInfo.StepRunNamespace),
		slog.String("mode", data.Execution.Mode),
		slog.Time("startedAt", data.StartedAt.Time),
	}
	if len(data.Inputs) > 0 {
		attrs = append(attrs,
			slog.Int("inputFields", len(data.Inputs)),
			debugPreviewAttr("inputsPreview", data.Inputs),
		)
	}
	if len(data.Config) > 0 {
		attrs = append(attrs,
			slog.Int("configFields", len(data.Config)),
			debugPreviewAttr("configPreview", data.Config),
		)
	}
	if len(data.Transports) > 0 {
		names := make([]string, 0, len(data.Transports))
		for _, t := range data.Transports {
			if strings.TrimSpace(t.Name) != "" {
				names = append(names, strings.TrimSpace(t.Name))
			}
		}
		if len(names) > 0 {
			sort.Strings(names)
			attrs = append(attrs, slog.Any("transports", names))
		}
	}
	if len(data.Secrets) > 0 {
		attrs = append(attrs, secretKeysAttr("secretKeys", data.Secrets))
	}
	if data.Storage != nil {
		attrs = append(attrs, slog.String("storageProvider", data.Storage.Provider))
	}
	args := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		args = append(args, attr)
	}
	logger.Debug("Execution context hydrated", args...)
}

func debugPreviewAttr(key string, value any) slog.Attr {
	if value == nil {
		return slog.String(key, "<nil>")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return slog.String(key, fmt.Sprintf("<marshal error: %v>", err))
	}
	return slog.String(key, truncateDebugPayload(payload))
}

func debugBytesAttr(key string, data []byte) slog.Attr {
	if len(data) == 0 {
		return slog.String(key, "<empty>")
	}
	return slog.String(key, truncateDebugPayload(data))
}

func secretKeysAttr(key string, secrets map[string]string) slog.Attr {
	if len(secrets) == 0 {
		return slog.Any(key, []string{})
	}
	keys := make([]string, 0, len(secrets))
	for k := range secrets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return slog.Any(key, keys)
}

func truncateDebugPayload(data []byte) string {
	if len(data) <= debugPreviewLimit {
		return string(data)
	}
	return string(data[:debugPreviewLimit]) + "...(truncated)"
}
