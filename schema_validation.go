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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
	"unicode"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/k8s"
	sdkenv "github.com/bubustack/bubu-sdk-go/pkg/env"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/core/contracts"
	"github.com/xeipuuv/gojsonschema"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type k8sGetter interface {
	Get(context.Context, types.NamespacedName, client.Object, ...client.GetOption) error
}

const (
	defaultSchemaLookupTimeout        = 30 * time.Second
	schemaAllowRuntimeRefsKeyword     = "x-bubu-allow-runtime-refs"
	schemaAllowTemplateStringsKeyword = "x-bubu-allow-template-strings"
	templateStringPattern             = `^\s*\$?\{\{[\s\S]+\}\}\s*$`
)

type schemaNormalizationOptions struct {
	allowRuntimeRefs     bool
	allowTemplateStrings bool
}

func validateBatchInputs(
	ctx context.Context,
	k8sClient K8sClient,
	execCtxData *runtime.ExecutionContextData,
	inputs map[string]any,
) error {
	logger := LoggerFromContext(ctx)
	schema, schemaName, ok, err := resolveInputSchema(ctx, k8sClient, execCtxData, logger)
	if err != nil {
		return NewStructuredError(
			runsv1alpha1.StructuredErrorTypeExecution,
			fmt.Sprintf("input schema resolution failed: %v", err),
			WithStructuredErrorCause(err),
			WithStructuredErrorRetryable(true),
			WithStructuredErrorExitClass(enums.ExitClassRetry),
		)
	}
	if !ok {
		return nil
	}
	inputBytes, err := json.Marshal(inputs)
	if err != nil {
		return fmt.Errorf("failed to marshal inputs for schema validation: %w", err)
	}
	if err := validateJSONInputsBytes(inputBytes, schema, schemaName); err != nil {
		return NewStructuredError(
			runsv1alpha1.StructuredErrorTypeValidation,
			fmt.Sprintf("input schema validation failed: %v", err),
			WithStructuredErrorCause(err),
			WithStructuredErrorExitClass(enums.ExitClassTerminal),
		)
	}
	return nil
}

func validateBatchOutputs(
	ctx context.Context,
	k8sClient K8sClient,
	execCtxData *runtime.ExecutionContextData,
	outputBytes []byte,
) error {
	logger := LoggerFromContext(ctx)
	schema, schemaName, ok, err := resolveOutputSchema(ctx, k8sClient, execCtxData, logger)
	if err != nil {
		return NewStructuredError(
			runsv1alpha1.StructuredErrorTypeExecution,
			fmt.Sprintf("output schema resolution failed: %v", err),
			WithStructuredErrorCause(err),
			WithStructuredErrorRetryable(true),
			WithStructuredErrorExitClass(enums.ExitClassRetry),
		)
	}
	if !ok {
		return nil
	}
	if err := validateJSONOutputBytes(outputBytes, schema, schemaName); err != nil {
		return NewStructuredError(
			runsv1alpha1.StructuredErrorTypeValidation,
			fmt.Sprintf("output schema validation failed: %v", err),
			WithStructuredErrorCause(err),
			WithStructuredErrorExitClass(enums.ExitClassTerminal),
		)
	}
	return nil
}

func resolveInputSchema(
	ctx context.Context,
	k8sClient K8sClient,
	execCtxData *runtime.ExecutionContextData,
	logger *slog.Logger,
) (*k8sruntime.RawExtension, string, bool, error) {
	template, ok, err := fetchEngramTemplate(ctx, k8sClient, execCtxData, logger)
	if err != nil {
		return nil, "", false, err
	}
	if !ok || template == nil {
		return nil, "", false, nil
	}
	if template.Spec.InputSchema == nil || len(template.Spec.InputSchema.Raw) == 0 {
		return nil, "", false, nil
	}
	schemaName := fmt.Sprintf("EngramTemplate %s inputs", template.Name)
	return template.Spec.InputSchema, schemaName, true, nil
}

func resolveOutputSchema(
	ctx context.Context,
	k8sClient K8sClient,
	execCtxData *runtime.ExecutionContextData,
	logger *slog.Logger,
) (*k8sruntime.RawExtension, string, bool, error) {
	template, ok, err := fetchEngramTemplate(ctx, k8sClient, execCtxData, logger)
	if err != nil {
		return nil, "", false, err
	}
	if !ok || template == nil {
		return nil, "", false, nil
	}
	if template.Spec.OutputSchema == nil || len(template.Spec.OutputSchema.Raw) == 0 {
		return nil, "", false, nil
	}
	schemaName := fmt.Sprintf("EngramTemplate %s output", template.Name)
	return template.Spec.OutputSchema, schemaName, true, nil
}

func fetchEngramTemplate(
	ctx context.Context,
	k8sClient K8sClient,
	execCtxData *runtime.ExecutionContextData,
	logger *slog.Logger,
) (*catalogv1alpha1.EngramTemplate, bool, error) {
	getter, ok := k8sClient.(k8sGetter)
	if !ok {
		logger.Debug("Schema validation skipped: k8s client does not support Get")
		return nil, false, nil
	}
	namespace := resolveSchemaNamespace(execCtxData)
	if namespace == "" {
		logger.Debug("Schema validation skipped: step run namespace unavailable")
		return nil, false, nil
	}
	lookupCtx, cancel := withSchemaLookupTimeout(ctx)
	defer cancel()
	engramName, err := resolveEngramName(lookupCtx, getter, execCtxData, namespace)
	if err != nil {
		return nil, false, fmt.Errorf("resolve engram name: %w", err)
	}
	if engramName == "" {
		logger.Debug("Schema validation skipped: engram name unavailable")
		return nil, false, nil
	}
	engram := &bubuv1alpha1.Engram{}
	if err := getter.Get(lookupCtx, types.NamespacedName{Name: engramName, Namespace: namespace}, engram); err != nil {
		if apierrors.IsForbidden(err) {
			if logger != nil {
				logger.Info(
					"Schema validation skipped: Engram lookup forbidden; controller-side validation remains authoritative",
					"engram", engramName,
					"namespace", namespace,
				)
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("fetch engram: %w", err)
	}
	templateName := strings.TrimSpace(engram.Spec.TemplateRef.Name)
	if templateName == "" {
		logger.Debug("Schema validation skipped: engram template name missing", "engram", engramName)
		return nil, false, nil
	}
	template := &catalogv1alpha1.EngramTemplate{}
	if err := getter.Get(lookupCtx, types.NamespacedName{Name: templateName}, template); err != nil {
		if apierrors.IsForbidden(err) {
			if logger != nil {
				logger.Info(
					"Schema validation skipped: EngramTemplate lookup forbidden; controller-side validation remains authoritative",
					"engram", engramName,
					"template", templateName,
				)
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("fetch engram template: %w", err)
	}
	return template, true, nil
}

func withSchemaLookupTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := sdkenv.GetDuration(contracts.K8sOperationTimeoutEnv, defaultSchemaLookupTimeout)
	return context.WithTimeout(ctx, timeout)
}

func resolveSchemaNamespace(execCtxData *runtime.ExecutionContextData) string {
	if execCtxData != nil {
		if ns := strings.TrimSpace(execCtxData.StoryInfo.StepRunNamespace); ns != "" {
			return ns
		}
	}
	return strings.TrimSpace(k8s.ResolvePodNamespace())
}

func resolveEngramName(
	ctx context.Context,
	getter k8sGetter,
	execCtxData *runtime.ExecutionContextData,
	namespace string,
) (string, error) {
	if name := strings.TrimSpace(os.Getenv(contracts.EngramNameEnv)); name != "" {
		return name, nil
	}
	if execCtxData == nil {
		return "", nil
	}
	stepRunName := strings.TrimSpace(execCtxData.StoryInfo.StepRunID)
	if stepRunName == "" {
		return "", nil
	}
	stepRun := &runsv1alpha1.StepRun{}
	if err := getter.Get(ctx, types.NamespacedName{Name: stepRunName, Namespace: namespace}, stepRun); err != nil {
		return "", err
	}
	if stepRun.Spec.EngramRef == nil {
		return "", nil
	}
	return strings.TrimSpace(stepRun.Spec.EngramRef.Name), nil
}

func validateJSONInputsBytes(input []byte, schema *k8sruntime.RawExtension, schemaName string) error {
	if schema == nil || len(schema.Raw) == 0 {
		return nil
	}
	trimmed := trimLeadingSpace(input)
	if len(trimmed) == 0 {
		trimmed = []byte("{}")
	}
	return validateJSONAgainstSchema(trimmed, schema.Raw, schemaName)
}

func validateJSONOutputBytes(output []byte, schema *k8sruntime.RawExtension, schemaName string) error {
	if schema == nil || len(schema.Raw) == 0 {
		return nil
	}
	trimmed := trimLeadingSpace(output)
	if len(trimmed) == 0 {
		return fmt.Errorf("%s output is empty but a schema is defined", schemaName)
	}
	return validateJSONAgainstSchema(trimmed, schema.Raw, schemaName)
}

func trimLeadingSpace(raw []byte) []byte {
	return bytes.TrimLeftFunc(raw, unicode.IsSpace)
}

func validateJSONAgainstSchema(doc []byte, schema []byte, schemaName string) error {
	normalizedSchema, err := normalizeSchemaBytes(schema)
	if err != nil {
		return fmt.Errorf("error validating against %s schema: failed to normalize schema: %w", schemaName, err)
	}
	schemaLoader := gojsonschema.NewStringLoader(string(normalizedSchema))
	documentLoader := gojsonschema.NewStringLoader(string(doc))
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return fmt.Errorf("error validating against %s schema: %w", schemaName, err)
	}
	if !result.Valid() {
		var errs []string
		for _, desc := range result.Errors() {
			errs = append(errs, desc.String())
		}
		return fmt.Errorf("object is invalid against %s schema: %v", schemaName, errs)
	}
	return nil
}

func normalizeSchemaBytes(schema []byte) ([]byte, error) {
	if len(schema) == 0 {
		return schema, nil
	}
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		return nil, err
	}
	if err := rejectExternalSchemaRefs(root); err != nil {
		return nil, err
	}
	normalized := normalizeSchemaNodeWithOptions(root, schemaNormalizationOptions{})
	out, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func rejectExternalSchemaRefs(node any) error {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			if key == "$ref" {
				ref, ok := value.(string)
				if ok {
					ref = strings.TrimSpace(ref)
					if ref != "" && !strings.HasPrefix(ref, "#") {
						return fmt.Errorf("external schema reference %q is not allowed", ref)
					}
				}
			}
			if err := rejectExternalSchemaRefs(value); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range typed {
			if err := rejectExternalSchemaRefs(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizeSchemaNodeWithOptions(node any, inherited schemaNormalizationOptions) any {
	switch typed := node.(type) {
	case map[string]any:
		options := schemaNormalizationOptionsForNode(typed, inherited)
		normalized := normalizeObjectSchema(typed, options)
		return allowRuntimeAlternatives(normalized, options)
	case []any:
		for i := range typed {
			typed[i] = normalizeSchemaNodeWithOptions(typed[i], inherited)
		}
		return typed
	default:
		return node
	}
}

func schemaNormalizationOptionsForNode(
	schema map[string]any,
	inherited schemaNormalizationOptions,
) schemaNormalizationOptions {
	options := inherited
	if raw, ok := schema[schemaAllowRuntimeRefsKeyword]; ok {
		if enabled, ok := raw.(bool); ok {
			options.allowRuntimeRefs = enabled
		}
	}
	if raw, ok := schema[schemaAllowTemplateStringsKeyword]; ok {
		if enabled, ok := raw.(bool); ok {
			options.allowTemplateStrings = enabled
		}
	}
	return options
}

func normalizeObjectSchema(obj map[string]any, options schemaNormalizationOptions) map[string]any {
	requiredSet := liftInlineRequiredFlags(obj, options)
	mergeRequiredSet(obj, requiredSet)
	normalizeNestedSchemaLocations(obj, options)
	return obj
}

func allowRuntimeAlternatives(schema map[string]any, options schemaNormalizationOptions) any { //nolint:gocyclo
	if schema == nil || len(schema) == 0 { //nolint:staticcheck
		return schema
	}
	if !options.allowRuntimeRefs && !options.allowTemplateStrings {
		return schema
	}
	if isImplicitRefSchema(schema) {
		return schema
	}
	if anyOf, ok := schema["anyOf"].([]any); ok {
		if options.allowRuntimeRefs && !schemaSliceHasImplicitRef(anyOf) {
			schema["anyOf"] = append(anyOf, storageRefSchema(), configMapRefSchema(), secretRefSchema())
		}
		if options.allowTemplateStrings && !schemaSliceHasTemplateString(schema["anyOf"].([]any)) {
			schema["anyOf"] = append(schema["anyOf"].([]any), templateStringSchema())
		}
		return schema
	}
	if oneOf, ok := schema["oneOf"].([]any); ok {
		if options.allowRuntimeRefs && !schemaSliceHasImplicitRef(oneOf) {
			schema["oneOf"] = append(oneOf, storageRefSchema(), configMapRefSchema(), secretRefSchema())
		}
		if options.allowTemplateStrings && !schemaSliceHasTemplateString(schema["oneOf"].([]any)) {
			schema["oneOf"] = append(schema["oneOf"].([]any), templateStringSchema())
		}
		return schema
	}

	alternatives := []any{schema}
	if options.allowRuntimeRefs {
		alternatives = append(alternatives, storageRefSchema(), configMapRefSchema(), secretRefSchema())
	}
	if options.allowTemplateStrings {
		alternatives = append(alternatives, templateStringSchema())
	}
	wrapped := map[string]any{"anyOf": alternatives}
	if title, ok := schema["title"]; ok {
		wrapped["title"] = title
	}
	if desc, ok := schema["description"]; ok {
		wrapped["description"] = desc
	}
	return wrapped
}

func schemaSliceHasImplicitRef(schemas []any) bool {
	for _, entry := range schemas {
		if schemaMap, ok := entry.(map[string]any); ok && schemaHasImplicitRefAlternative(schemaMap) {
			return true
		}
	}
	return false
}

func schemaSliceHasTemplateString(schemas []any) bool {
	for _, entry := range schemas {
		if schemaMap, ok := entry.(map[string]any); ok && schemaHasTemplateStringAlternative(schemaMap) {
			return true
		}
	}
	return false
}

func schemaHasImplicitRefAlternative(schema map[string]any) bool {
	if isImplicitRefSchema(schema) {
		return true
	}
	return schemaContainsAlternative(schema, schemaHasImplicitRefAlternative)
}

func schemaHasTemplateStringAlternative(schema map[string]any) bool {
	if isTemplateStringSchema(schema) {
		return true
	}
	return schemaContainsAlternative(schema, schemaHasTemplateStringAlternative)
}

func schemaContainsAlternative(schema map[string]any, predicate func(map[string]any) bool) bool {
	for _, key := range []string{"anyOf", "oneOf", "allOf"} {
		raw, ok := schema[key].([]any)
		if !ok {
			continue
		}
		for _, candidate := range raw {
			child, ok := candidate.(map[string]any)
			if ok && predicate(child) {
				return true
			}
		}
	}
	return false
}

func isImplicitRefSchema(schema map[string]any) bool {
	if isStorageRefSchema(schema) || isConfigMapRefSchema(schema) || isSecretRefSchema(schema) {
		return true
	}
	return false
}

func isStorageRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props[storage.StorageRefKey]
	return hasRef
}

func isConfigMapRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props["$bubuConfigMapRef"]
	return hasRef
}

func isSecretRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props["$bubuSecretRef"]
	return hasRef
}

func isTemplateStringSchema(schema map[string]any) bool {
	if schema["type"] != "string" {
		return false
	}
	pattern, hasPattern := schema["pattern"].(string)
	return hasPattern && pattern == templateStringPattern
}

func storageRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			storage.StorageRefKey:           map[string]any{"type": "string"},
			storage.StoragePathKey:          map[string]any{"type": "string"},
			storage.StorageContentTypeKey:   map[string]any{"type": "string"},
			storage.StorageSchemaKey:        map[string]any{"type": "string"},
			storage.StorageSchemaVersionKey: map[string]any{"type": "string"},
		},
		"required": []any{storage.StorageRefKey},
	}
}

func configMapRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"$bubuConfigMapRef": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"name":      map[string]any{"type": "string"},
							"key":       map[string]any{"type": "string"},
							"namespace": map[string]any{"type": "string"},
							"format": map[string]any{
								"type": "string",
								"enum": []any{"auto", "json", "raw"},
							},
						},
						"required": []any{"name", "key"},
					},
				},
			},
		},
		"required": []any{"$bubuConfigMapRef"},
	}
}

func secretRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"$bubuSecretRef": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"name":      map[string]any{"type": "string"},
							"key":       map[string]any{"type": "string"},
							"namespace": map[string]any{"type": "string"},
							"format": map[string]any{
								"type": "string",
								"enum": []any{"auto", "json", "raw"},
							},
						},
						"required": []any{"name", "key"},
					},
				},
			},
		},
		"required": []any{"$bubuSecretRef"},
	}
}

func templateStringSchema() map[string]any {
	return map[string]any{
		"type":    "string",
		"pattern": templateStringPattern,
	}
}

func liftInlineRequiredFlags(obj map[string]any, options schemaNormalizationOptions) map[string]struct{} {
	props, hasProps := obj["properties"].(map[string]any)
	if !hasProps {
		return nil
	}

	requiredSet := map[string]struct{}{}
	for propName, rawChild := range props {
		cleaned := stripBooleanRequired(rawChild, propName, requiredSet)
		props[propName] = normalizeSchemaNodeWithOptions(cleaned, options)
	}
	if len(requiredSet) == 0 {
		return nil
	}
	return requiredSet
}

func stripBooleanRequired(node any, propName string, requiredSet map[string]struct{}) any {
	childMap, ok := node.(map[string]any)
	if !ok {
		return node
	}
	if raw, has := childMap["required"]; has {
		if b, ok := raw.(bool); ok {
			if b {
				requiredSet[propName] = struct{}{}
			}
			delete(childMap, "required")
		}
	}
	return childMap
}

func mergeRequiredSet(obj map[string]any, requiredSet map[string]struct{}) {
	if len(requiredSet) == 0 {
		return
	}

	existingList := extractExistingRequired(obj)
	seen := make(map[string]struct{}, len(existingList))
	for _, name := range existingList {
		seen[name] = struct{}{}
	}

	for name := range requiredSet {
		if _, already := seen[name]; !already {
			existingList = append(existingList, name)
		}
	}

	out := make([]any, 0, len(existingList))
	for _, name := range existingList {
		out = append(out, name)
	}
	obj["required"] = out
}

func extractExistingRequired(obj map[string]any) []string {
	raw, has := obj["required"]
	if !has {
		return nil
	}

	switch typed := raw.(type) {
	case []any:
		var result []string
		for _, v := range typed {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return append([]string{}, typed...)
	default:
		return nil
	}
}

func normalizeNestedSchemaLocations(obj map[string]any, options schemaNormalizationOptions) {
	normalizeItemsNode(obj, options)
	normalizeSingleSchemaField(obj, "additionalProperties", options)
	normalizeMapOfSchemas(obj, "patternProperties", options)
	normalizeSchemaSlice(obj, "allOf", options)
	normalizeSchemaSlice(obj, "anyOf", options)
	normalizeSchemaSlice(obj, "oneOf", options)
	normalizeMapOfSchemas(obj, "definitions", options)
	normalizeMapOfSchemas(obj, "$defs", options)
	normalizeSingleSchemaField(obj, "not", options)
}

func normalizeItemsNode(obj map[string]any, options schemaNormalizationOptions) {
	items, has := obj["items"]
	if !has {
		return
	}
	switch typed := items.(type) {
	case map[string]any, []any:
		obj["items"] = normalizeSchemaNodeWithOptions(typed, options)
	}
}

func normalizeSingleSchemaField(obj map[string]any, key string, options schemaNormalizationOptions) {
	if raw, has := obj[key]; has {
		if schemaMap, ok := raw.(map[string]any); ok {
			obj[key] = normalizeSchemaNodeWithOptions(schemaMap, options)
		}
	}
}

func normalizeMapOfSchemas(obj map[string]any, key string, options schemaNormalizationOptions) {
	raw, has := obj[key].(map[string]any)
	if !has {
		return
	}
	for k, v := range raw {
		raw[k] = normalizeSchemaNodeWithOptions(v, options)
	}
}

func normalizeSchemaSlice(obj map[string]any, key string, options schemaNormalizationOptions) {
	raw, has := obj[key].([]any)
	if !has {
		return
	}
	childOptions := options
	if key == "oneOf" {
		// Preserve oneOf exclusivity: inherited runtime/template broadening is
		// applied once at the parent oneOf node, not injected into every branch.
		childOptions.allowRuntimeRefs = false
		childOptions.allowTemplateStrings = false
	}
	for i := range raw {
		raw[i] = normalizeSchemaNodeWithOptions(raw[i], childOptions)
	}
	obj[key] = raw
}
