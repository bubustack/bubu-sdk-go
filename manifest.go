/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/engram"
)

func buildManifestData(
	result *engram.Result,
	requests []runsv1alpha1.ManifestRequest,
) (map[string]runsv1alpha1.StepManifestData, []string) {
	if len(requests) == 0 {
		return nil, nil
	}

	manifest := make(map[string]runsv1alpha1.StepManifestData, len(requests))
	var warnings []string

	var rawData any
	if result != nil {
		rawData = result.Data
	}

	for _, req := range requests {
		path := req.Path
		if path == "" {
			path = "$"
		}

		ops := req.Operations
		if len(ops) == 0 {
			ops = []runsv1alpha1.ManifestOperation{runsv1alpha1.ManifestOperationExists}
		}

		value, exists, err := resolveManifestPath(rawData, path)
		entry := runsv1alpha1.StepManifestData{}
		if containsManifestOp(ops, runsv1alpha1.ManifestOperationExists) {
			existsCopy := exists
			entry.Exists = &existsCopy
		}

		if containsManifestOp(ops, runsv1alpha1.ManifestOperationLength) {
			if !exists {
				zero := int64(0)
				entry.Length = &zero
			} else {
				length, lengthErr := computeManifestLength(value)
				if lengthErr != nil {
					entry.Error = lengthErr.Error()
					warnings = append(warnings, fmt.Sprintf("%s: %v", path, lengthErr))
				} else if length != nil {
					entry.Length = length
				}
			}
		}

		if err != nil {
			entry.Error = err.Error()
			warnings = append(warnings, fmt.Sprintf("%s: %v", path, err))
		}

		manifest[path] = entry
	}

	return manifest, warnings
}

func containsManifestOp(ops []runsv1alpha1.ManifestOperation, target runsv1alpha1.ManifestOperation) bool {
	for _, op := range ops {
		if op == target {
			return true
		}
	}
	return false
}

type manifestPathToken struct {
	key   string
	index *int
}

func resolveManifestPath(data any, path string) (any, bool, error) {
	if path == "" || path == "$" {
		if data == nil {
			return nil, false, nil
		}
		return data, true, nil
	}

	tokens, err := parseManifestPath(path)
	if err != nil {
		return nil, false, err
	}

	current := data
	for _, token := range tokens {
		if token.key != "" {
			m, ok := toStringMap(current)
			if !ok {
				return nil, false, fmt.Errorf("expected map when accessing key %q", token.key)
			}
			val, exists := m[token.key]
			if !exists {
				return nil, false, nil
			}
			current = val
		}
		if token.index != nil {
			slice, ok := toSlice(current)
			if !ok {
				return nil, false, fmt.Errorf("expected list when accessing index %d", *token.index)
			}
			if *token.index < 0 || *token.index >= len(slice) {
				return nil, false, nil
			}
			current = slice[*token.index]
		}
	}

	if current == nil {
		return nil, false, nil
	}
	return current, true, nil
}

func parseManifestPath(path string) ([]manifestPathToken, error) {
	parts := strings.Split(path, ".")
	tokens := make([]manifestPathToken, 0)

	for _, part := range parts {
		if part == "" {
			continue
		}
		remainder := part
		for len(remainder) > 0 {
			if remainder[0] == '[' {
				closing := strings.IndexByte(remainder, ']')
				if closing == -1 {
					return nil, fmt.Errorf("unmatched '[' in manifest path %q", path)
				}
				idxStr := remainder[1:closing]
				idx, err := strconv.Atoi(idxStr)
				if err != nil {
					return nil, fmt.Errorf("invalid index %q in manifest path %q", idxStr, path)
				}
				idxCopy := idx
				tokens = append(tokens, manifestPathToken{index: &idxCopy})
				remainder = remainder[closing+1:]
				continue
			}

			nextBracket := strings.IndexByte(remainder, '[')
			if nextBracket == -1 {
				tokens = append(tokens, manifestPathToken{key: remainder})
				remainder = ""
			} else {
				key := remainder[:nextBracket]
				tokens = append(tokens, manifestPathToken{key: key})
				remainder = remainder[nextBracket:]
			}
		}
	}

	return tokens, nil
}

func toStringMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, false
	}

	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		result := make(map[string]any, rv.Len())
		for _, key := range rv.MapKeys() {
			result[key.String()] = rv.MapIndex(key).Interface()
		}
		return result, true
	}

	if rv.Kind() == reflect.Struct {
		bytes, err := json.Marshal(value)
		if err != nil {
			return nil, false
		}
		var result map[string]any
		if err := json.Unmarshal(bytes, &result); err != nil {
			return nil, false
		}
		return result, true
	}

	return nil, false
}

func toSlice(value any) ([]any, bool) {
	if typed, ok := value.([]any); ok {
		return typed, true
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, false
	}

	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		result := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			result[i] = rv.Index(i).Interface()
		}
		return result, true
	}
	return nil, false
}

func computeManifestLength(value any) (*int64, error) {
	if value == nil {
		zero := int64(0)
		return &zero, nil
	}

	switch typed := value.(type) {
	case string:
		length := int64(len(typed))
		return &length, nil
	case []byte:
		length := int64(len(typed))
		return &length, nil
	case []any:
		length := int64(len(typed))
		return &length, nil
	case map[string]any:
		length := int64(len(typed))
		return &length, nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.Map {
		length := int64(rv.Len())
		return &length, nil
	}

	return nil, fmt.Errorf("length not supported for type %T", value)
}
