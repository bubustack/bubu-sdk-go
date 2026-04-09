package sdk

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/bubustack/bobrapet/pkg/storage"
	sdkenv "github.com/bubustack/bubu-sdk-go/pkg/env"
)

const (
	defaultConfigHydrationMaxDepth = 64
	defaultConfigHydrationMaxNodes = 10000
	configHydrationMaxDepthEnv     = "BUBU_CONFIG_HYDRATION_MAX_DEPTH"
	configHydrationMaxNodesEnv     = "BUBU_CONFIG_HYDRATION_MAX_NODES"
)

type configPathSegment struct {
	key     string
	index   int
	isIndex bool
}

type configPath []configPathSegment

type extractedConfigValue struct {
	path  configPath
	value any
}

type configTraversalKey struct {
	kind reflect.Kind
	ptr  uintptr
}

type configTraversalGuard struct {
	maxDepth int
	maxNodes int
	nodes    int
	stack    map[configTraversalKey]struct{}
}

func hydrateConfig(
	ctx context.Context,
	sm StorageManager,
	config map[string]any,
	celContext map[string]any) (map[string]any,
	error,
) {
	if len(config) == 0 {
		return config, nil
	}
	if sm == nil {
		return nil, fmt.Errorf("storage manager is required to hydrate config")
	}

	logger := LoggerFromContext(ctx)
	start := time.Now()
	logger.Debug("Hydrating config",
		slog.Int("configFields", len(config)),
		slog.Int("configBytes", estimateJSONSize(config)),
	)

	if err := validateConfigTraversalSafety(config); err != nil {
		return nil, fmt.Errorf("config rejected before hydration: %w", err)
	}

	// Record which config paths contain storage refs before hydration.
	// After hydration these become raw user data (e.g. RSS bodies) that may
	// contain literal "{{ ... }}" strings. We must skip template evaluation
	// on these values to avoid treating user data as templates.
	storageRefPaths := findStorageRefPaths(config)

	hydrated, err := sm.Hydrate(ctx, config)
	if err != nil {
		logger.Error("Config storage hydration failed", "error", err, slog.Duration("duration", time.Since(start)))
		return nil, err
	}
	if hydrated == nil {
		return nil, nil
	}
	hydratedMap, ok := hydrated.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("hydrated config must be an object, got %T", hydrated)
	}
	if err := validateConfigTraversalSafety(hydratedMap); err != nil {
		return nil, fmt.Errorf("hydrated config rejected before template resolution: %w", err)
	}
	logger.Debug("Config storage refs hydrated",
		slog.Duration("duration", time.Since(start)),
		slog.Int("hydratedFields", len(hydratedMap)),
		slog.Int("hydratedBytes", estimateJSONSize(hydratedMap)),
	)

	// Extract storage-ref-hydrated values before template evaluation so
	// raw user data is never parsed as Go templates.
	extracted := extractPaths(hydratedMap, storageRefPaths)
	if len(extracted) > 0 {
		logger.Debug("Extracted storage-ref values from template evaluation",
			slog.Int("count", len(extracted)),
		)
	}

	resolved, err := resolveCELTemplates(ctx, logger, sm, celContext, hydratedMap)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve config templates: %w", err)
	}
	if resolved == nil {
		return map[string]any{}, nil
	}
	resolvedMap, ok := resolved.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("resolved config must be an object, got %T", resolved)
	}

	// Restore extracted storage-ref values into the resolved config.
	restorePaths(resolvedMap, extracted)

	logger.Debug("Config hydration completed",
		slog.Duration("totalDuration", time.Since(start)),
		slog.Int("resolvedFields", len(resolvedMap)),
	)
	return resolvedMap, nil
}

func validateConfigTraversalSafety(config map[string]any) error {
	guard := configTraversalGuard{
		maxDepth: sdkenv.GetInt(configHydrationMaxDepthEnv, defaultConfigHydrationMaxDepth),
		maxNodes: sdkenv.GetInt(configHydrationMaxNodesEnv, defaultConfigHydrationMaxNodes),
		stack:    make(map[configTraversalKey]struct{}),
	}
	return guard.walk(config, 0)
}

func (g *configTraversalGuard) walk(value any, depth int) error {
	if depth > g.maxDepth {
		return fmt.Errorf("config exceeds max depth %d", g.maxDepth)
	}
	g.nodes++
	if g.nodes > g.maxNodes {
		return fmt.Errorf("config exceeds max nodes %d", g.maxNodes)
	}

	switch typed := value.(type) {
	case map[string]any:
		key := configMapTraversalKey(typed)
		if err := g.push(key); err != nil {
			return err
		}
		defer g.pop(key)
		for _, child := range typed {
			if err := g.walk(child, depth+1); err != nil {
				return err
			}
		}
	case []any:
		key := configSliceTraversalKey(typed)
		if err := g.push(key); err != nil {
			return err
		}
		defer g.pop(key)
		for _, child := range typed {
			if err := g.walk(child, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *configTraversalGuard) push(key configTraversalKey) error {
	if key.ptr == 0 {
		return nil
	}
	if _, exists := g.stack[key]; exists {
		return fmt.Errorf("config contains cycle")
	}
	g.stack[key] = struct{}{}
	return nil
}

func (g *configTraversalGuard) pop(key configTraversalKey) {
	if key.ptr == 0 {
		return
	}
	delete(g.stack, key)
}

func configMapTraversalKey(value map[string]any) configTraversalKey {
	return configTraversalKey{
		kind: reflect.Map,
		ptr:  reflect.ValueOf(value).Pointer(),
	}
}

func configSliceTraversalKey(value []any) configTraversalKey {
	if len(value) == 0 {
		return configTraversalKey{}
	}
	return configTraversalKey{
		kind: reflect.Slice,
		ptr:  reflect.ValueOf(value).Pointer(),
	}
}

// findStorageRefPaths walks a config map and returns exact paths to values that
// are direct $bubuStorageRef objects. These exact values will become raw user
// data after hydration and must not be template-evaluated.
func findStorageRefPaths(config map[string]any) []configPath {
	paths := make([]configPath, 0, len(config))
	for key, val := range config {
		paths = append(paths, findStorageRefPathsInValue(val, configPath{{key: key}})...)
	}
	return paths
}

func findStorageRefPathsInValue(v any, path configPath) []configPath {
	switch val := v.(type) {
	case map[string]any:
		if _, ok := val[storage.StorageRefKey]; ok {
			return []configPath{cloneConfigPath(path)}
		}
		var paths []configPath
		for key, nested := range val {
			paths = append(paths, findStorageRefPathsInValue(nested, appendConfigPathKey(path, key))...)
		}
		return paths
	case []any:
		var paths []configPath
		for idx, item := range val {
			paths = append(paths, findStorageRefPathsInValue(item, appendConfigPathIndex(path, idx))...)
		}
		return paths
	}
	return nil
}

func cloneConfigPath(path configPath) configPath {
	if len(path) == 0 {
		return nil
	}
	cloned := make(configPath, len(path))
	copy(cloned, path)
	return cloned
}

func appendConfigPathKey(path configPath, key string) configPath {
	cloned := make(configPath, len(path)+1)
	copy(cloned, path)
	cloned[len(path)] = configPathSegment{key: key}
	return cloned
}

func appendConfigPathIndex(path configPath, index int) configPath {
	cloned := make(configPath, len(path)+1)
	copy(cloned, path)
	cloned[len(path)] = configPathSegment{index: index, isIndex: true}
	return cloned
}

// extractPaths replaces the given paths with inert placeholders and returns the
// original values for later restoration after template evaluation.
func extractPaths(m map[string]any, paths []configPath) []extractedConfigValue {
	if len(paths) == 0 {
		return nil
	}
	extracted := make([]extractedConfigValue, 0, len(paths))
	for idx, path := range paths {
		if val, ok := replacePathWithPlaceholder(m, path, pathPlaceholder(idx)); ok {
			extracted = append(extracted, extractedConfigValue{
				path:  cloneConfigPath(path),
				value: val,
			})
		}
	}
	return extracted
}

func pathPlaceholder(idx int) string {
	return fmt.Sprintf("__bubu_storage_ref_skip_%d__", idx)
}

func replacePathWithPlaceholder(m map[string]any, path configPath, placeholder any) (any, bool) {
	var current any = m
	for idx, segment := range path {
		last := idx == len(path)-1
		if segment.isIndex {
			items, ok := current.([]any)
			if !ok || segment.index < 0 || segment.index >= len(items) {
				return nil, false
			}
			if last {
				value := items[segment.index]
				items[segment.index] = placeholder
				return value, true
			}
			current = items[segment.index]
			continue
		}

		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := obj[segment.key]
		if !ok {
			return nil, false
		}
		if last {
			obj[segment.key] = placeholder
			return value, true
		}
		current = value
	}
	return nil, false
}

// restorePaths puts extracted values back into the config map.
func restorePaths(m map[string]any, extracted []extractedConfigValue) {
	for _, item := range extracted {
		restorePath(m, item.path, item.value)
	}
}

func restorePath(m map[string]any, path configPath, value any) {
	var current any = m
	for idx, segment := range path {
		last := idx == len(path)-1
		if segment.isIndex {
			items, ok := current.([]any)
			if !ok || segment.index < 0 || segment.index >= len(items) {
				return
			}
			if last {
				items[segment.index] = value
				return
			}
			current = items[segment.index]
			continue
		}

		obj, ok := current.(map[string]any)
		if !ok {
			return
		}
		if last {
			obj[segment.key] = value
			return
		}
		next, ok := obj[segment.key]
		if !ok {
			return
		}
		current = next
	}
}
