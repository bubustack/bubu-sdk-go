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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type identityStorageManager struct{}

func (identityStorageManager) Hydrate(ctx context.Context, data any) (any, error) {
	return data, nil
}

func (identityStorageManager) Dehydrate(ctx context.Context, data any, stepRunID string) (any, error) {
	return data, nil
}

type schemaK8sClient struct {
	client.Client
	patched []runsv1alpha1.StepRunStatus
}

type failingSchemaClient struct {
	patched []runsv1alpha1.StepRunStatus
	err     error
}

type deadlineRecordingSchemaClient struct {
	client.Client
	deadlinesSeen []bool
}

type blockingSchemaClient struct{}
type templateForbiddenSchemaClient struct {
	client.Client
	err error
}

func (s *schemaK8sClient) TriggerStory(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	return nil, nil
}

func (s *schemaK8sClient) PatchStepRunStatus(
	ctx context.Context,
	stepRunName string,
	patchData runsv1alpha1.StepRunStatus,
) error {
	s.patched = append(s.patched, patchData)
	return nil
}

func (s *failingSchemaClient) TriggerStory(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	return nil, nil
}

func (s *failingSchemaClient) PatchStepRunStatus(
	ctx context.Context,
	stepRunName string,
	patchData runsv1alpha1.StepRunStatus,
) error {
	s.patched = append(s.patched, patchData)
	return nil
}

func (s *failingSchemaClient) Get(ctx context.Context, key types.NamespacedName,
	obj client.Object, opts ...client.GetOption) error {
	return s.err
}

func (s *deadlineRecordingSchemaClient) TriggerStory(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	return nil, nil
}

func (s *deadlineRecordingSchemaClient) PatchStepRunStatus(
	ctx context.Context,
	stepRunName string,
	patchData runsv1alpha1.StepRunStatus,
) error {
	return nil
}

func (s *deadlineRecordingSchemaClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error { //nolint:lll
	_, ok := ctx.Deadline()
	s.deadlinesSeen = append(s.deadlinesSeen, ok)
	return s.Client.Get(ctx, key, obj, opts...)
}

func (blockingSchemaClient) TriggerStory(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	return nil, nil
}

func (blockingSchemaClient) PatchStepRunStatus(
	ctx context.Context,
	stepRunName string,
	patchData runsv1alpha1.StepRunStatus,
) error {
	return nil
}

func (blockingSchemaClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error { //nolint:lll
	<-ctx.Done()
	return ctx.Err()
}

func (s *templateForbiddenSchemaClient) TriggerStory(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	return nil, nil
}

func (s *templateForbiddenSchemaClient) PatchStepRunStatus(
	ctx context.Context,
	stepRunName string,
	patchData runsv1alpha1.StepRunStatus,
) error {
	return nil
}

func (s *templateForbiddenSchemaClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error { //nolint:lll
	switch obj.(type) {
	case *catalogv1alpha1.EngramTemplate:
		return s.err
	default:
		return s.Client.Get(ctx, key, obj, opts...)
	}
}

func newSchemaClient(t *testing.T, objects ...client.Object) *schemaK8sClient {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))
	return &schemaK8sClient{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			Build(),
	}
}

func TestHydrateAndUnmarshalInputs_ValidatesSchema(t *testing.T) {
	t.Setenv(contracts.EngramNameEnv, "engram")
	t.Setenv(contracts.SkipInputTemplatingEnv, "true")

	inputSchema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"foo": map[string]any{"type": "string"},
		},
		"required": []any{"foo"},
	})
	require.NoError(t, err)

	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "template",
		},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			InputSchema: &k8sruntime.RawExtension{Raw: inputSchema},
		},
	}
	engramObj := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engram",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}

	k8sClient := newSchemaClient(t, template, engramObj)

	execCtx := &runtime.ExecutionContextData{
		Inputs: map[string]any{"foo": 123},
		StoryInfo: engram.StoryInfo{
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
		StartedAt: metav1.Now(),
	}

	_, err = hydrateAndUnmarshalInputs[struct{}, map[string]any](context.Background(), identityStorageManager{}, k8sClient, execCtx) //nolint:lll
	require.Error(t, err)
	require.Len(t, k8sClient.patched, 1)

	status := k8sClient.patched[0]
	require.Equal(t, enums.PhaseFailed, status.Phase)
	serr := requireStructuredStatusError(t, &status)
	require.Equal(t, runsv1alpha1.StructuredErrorTypeValidation, serr.Type)
}

func TestHandleResultAndPatchStatus_ValidatesOutputSchema(t *testing.T) {
	t.Setenv(contracts.EngramNameEnv, "engram")

	outputSchema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"foo": map[string]any{"type": "string"},
		},
		"required": []any{"foo"},
	})
	require.NoError(t, err)

	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "template",
		},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			OutputSchema: &k8sruntime.RawExtension{Raw: outputSchema},
		},
	}
	engramObj := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engram",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}

	k8sClient := newSchemaClient(t, template, engramObj)

	execCtx := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
		StartedAt: metav1.Now(),
	}

	result := engram.NewResultFrom(map[string]any{"foo": 123})
	succeeded, _, finalErr, patchErr := handleResultAndPatchStatus(
		context.Background(),
		identityStorageManager{},
		k8sClient,
		execCtx,
		result,
		nil,
		false,
		nil,
	)

	require.False(t, succeeded)
	require.Error(t, finalErr)
	require.NoError(t, patchErr)
	require.Len(t, k8sClient.patched, 1)

	status := k8sClient.patched[0]
	require.Equal(t, enums.PhaseFailed, status.Phase)
	serr := requireStructuredStatusError(t, &status)
	require.Equal(t, runsv1alpha1.StructuredErrorTypeValidation, serr.Type)
}

func TestHydrateAndUnmarshalInputs_FailsClosedWhenSchemaLookupErrors(t *testing.T) {
	t.Setenv(contracts.EngramNameEnv, "engram")
	t.Setenv(contracts.SkipInputTemplatingEnv, "true")

	k8sClient := &failingSchemaClient{err: fmt.Errorf("apiserver unavailable")}
	execCtx := &runtime.ExecutionContextData{
		Inputs: map[string]any{"foo": "bar"},
		StoryInfo: engram.StoryInfo{
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
		StartedAt: metav1.Now(),
	}

	_, err := hydrateAndUnmarshalInputs[struct{}, map[string]any](context.Background(), identityStorageManager{}, k8sClient, execCtx) //nolint:lll
	require.Error(t, err)
	require.Len(t, k8sClient.patched, 1)

	status := k8sClient.patched[0]
	require.Equal(t, enums.PhaseFailed, status.Phase)
	require.Equal(t, enums.ExitClassRetry, status.ExitClass)
	serr := requireStructuredStatusError(t, &status)
	require.Equal(t, runsv1alpha1.StructuredErrorTypeExecution, serr.Type)
	require.Equal(t, runsv1alpha1.StructuredErrorExitClass(enums.ExitClassRetry), serr.ExitClass)
}

func TestResolveInputSchema_AppliesBoundedDeadlineToLookupGets(t *testing.T) {
	inputSchema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"foo": map[string]any{"type": "string"},
		},
	})
	require.NoError(t, err)

	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			InputSchema: &k8sruntime.RawExtension{Raw: inputSchema},
		},
	}
	engramObj := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engram",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-run",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "engram"},
			},
		},
	}

	baseClient := newSchemaClient(t, template, engramObj, stepRun)
	recordingClient := &deadlineRecordingSchemaClient{Client: baseClient.Client}
	execCtx := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
	}

	schema, schemaName, ok, err := resolveInputSchema(context.Background(), recordingClient, execCtx, nil) //nolint:revive
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, schema)
	require.Equal(t, "EngramTemplate template inputs", schemaName)
	require.Len(t, recordingClient.deadlinesSeen, 3)
	for i, sawDeadline := range recordingClient.deadlinesSeen {
		require.Truef(t, sawDeadline, "expected lookup %d to use a bounded context", i+1)
	}
}

func TestFetchEngramTemplate_BlocksOnlyUntilSchemaLookupTimeout(t *testing.T) {
	t.Setenv(contracts.EngramNameEnv, "engram")
	t.Setenv(contracts.K8sOperationTimeoutEnv, "10ms")

	execCtx := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{
			StepRunNamespace: "default",
		},
	}

	type result struct {
		template *catalogv1alpha1.EngramTemplate
		ok       bool
		err      error
	}
	done := make(chan result, 1)
	go func() {
		template, ok, err := fetchEngramTemplate(context.Background(), blockingSchemaClient{}, execCtx, nil)
		done <- result{template: template, ok: ok, err: err}
	}()

	select {
	case res := <-done:
		require.Nil(t, res.template)
		require.False(t, res.ok)
		require.Error(t, res.err)
		require.True(t, errors.Is(res.err, context.DeadlineExceeded), "expected deadline exceeded, got %v", res.err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("schema lookup did not honor bounded timeout")
	}
}

func TestFetchEngramTemplate_SkipsForbiddenEngramLookup(t *testing.T) {
	t.Setenv(contracts.EngramNameEnv, "engram")

	execCtx := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{
			StepRunNamespace: "default",
		},
	}
	k8sClient := &failingSchemaClient{
		err: apierrors.NewForbidden(
			schema.GroupResource{Group: "bubustack.io", Resource: "engrams"},
			"engram",
			errors.New("denied"),
		),
	}

	template, ok, err := fetchEngramTemplate(context.Background(), k8sClient, execCtx, nil)
	require.Nil(t, template)
	require.False(t, ok)
	require.NoError(t, err)
}

func TestFetchEngramTemplate_SkipsForbiddenTemplateLookup(t *testing.T) {
	t.Setenv(contracts.EngramNameEnv, "engram")

	execCtx := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{
			StepRunNamespace: "default",
		},
	}
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
	}
	engramObj := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}

	base := newSchemaClient(t, engramObj, template)
	k8sClient := &templateForbiddenSchemaClient{
		Client: base.Client,
		err: apierrors.NewForbidden(
			schema.GroupResource{Group: "catalog.bubustack.io", Resource: "engramtemplates"},
			"template",
			errors.New("denied"),
		),
	}

	resolved, ok, err := fetchEngramTemplate(context.Background(), k8sClient, execCtx, nil)
	require.Nil(t, resolved)
	require.False(t, ok)
	require.NoError(t, err)
}

func TestFetchEngramTemplate_IgnoresLegacySkipEnvAndStillFailsClosed(t *testing.T) {
	t.Setenv("BUBU_SCHEMA_ALLOW_LOOKUP_SKIP_ON_ERROR", "true")

	execCtx := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
	}
	k8sClient := &failingSchemaClient{
		err: apierrors.NewNotFound(
			schema.GroupResource{Group: "bubustack.io", Resource: "stepruns"},
			"step-run",
		),
	}

	template, ok, err := fetchEngramTemplate(context.Background(), k8sClient, execCtx, nil)
	require.Nil(t, template)
	require.False(t, ok)
	require.Error(t, err)
	require.ErrorContains(t, err, "resolve engram name")
}

func TestValidateJSONAgainstSchemaRejectsExternalRefs(t *testing.T) {
	err := validateJSONAgainstSchema(
		[]byte(`{"foo":"bar"}`),
		[]byte(`{"$ref":"https://example.com/schema.json"}`),
		"test",
	)
	require.ErrorContains(t, err, "external schema reference")
}

func TestValidateJSONAgainstSchema_RejectsImplicitRuntimeAlternativesByDefault(t *testing.T) {
	schemaBytes, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"secret": map[string]any{"type": "string"},
			"color": map[string]any{
				"type": "string",
				"enum": []any{"red", "blue"},
			},
		},
		"required": []any{"secret", "color"},
	})
	require.NoError(t, err)

	err = validateJSONAgainstSchema(
		[]byte(`{"secret":{"$bubuSecretRef":"ns/name:key"},"color":"red"}`),
		schemaBytes,
		"test",
	)
	require.Error(t, err)

	err = validateJSONAgainstSchema(
		[]byte(`{"secret":"plain","color":"{{ .inputs.color }}"}`),
		schemaBytes,
		"test",
	)
	require.Error(t, err)
}

func TestValidateJSONAgainstSchema_AllowsRuntimeAlternativesWhenSchemaOptsIn(t *testing.T) {
	schemaBytes, err := json.Marshal(map[string]any{
		"type":                            "object",
		schemaAllowRuntimeRefsKeyword:     true,
		schemaAllowTemplateStringsKeyword: true,
		"properties": map[string]any{
			"secret": map[string]any{"type": "string"},
			"color": map[string]any{
				"type": "string",
				"enum": []any{"red", "blue"},
			},
		},
		"required": []any{"secret", "color"},
	})
	require.NoError(t, err)

	err = validateJSONAgainstSchema(
		[]byte(`{"secret":{"$bubuSecretRef":"ns/name:key"},"color":"{{ .inputs.color }}"}`),
		schemaBytes,
		"test",
	)
	require.NoError(t, err)
}

func TestValidateJSONAgainstSchema_AddsTemplateAlternativeWhenOneOfHasNonTemplatePattern(t *testing.T) {
	schemaBytes, err := json.Marshal(map[string]any{
		"type":                            "object",
		schemaAllowTemplateStringsKeyword: true,
		"properties": map[string]any{
			"color": map[string]any{
				"oneOf": []any{
					map[string]any{
						"type":    "string",
						"pattern": "^[a-z]+$",
					},
					map[string]any{
						"type": "integer",
					},
				},
			},
		},
		"required": []any{"color"},
	})
	require.NoError(t, err)

	err = validateJSONAgainstSchema(
		[]byte(`{"color":"{{ .inputs.color }}"}`),
		schemaBytes,
		"test",
	)
	require.NoError(t, err)
}

func TestValidateJSONAgainstSchema_FieldOptInDoesNotBroadenSiblingFields(t *testing.T) {
	schemaBytes, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"allowedSecret": map[string]any{
				"type":                        "string",
				schemaAllowRuntimeRefsKeyword: true,
			},
			"allowedColor": map[string]any{
				"type":                            "string",
				"enum":                            []any{"red", "blue"},
				schemaAllowTemplateStringsKeyword: true,
			},
			"literalSecret": map[string]any{"type": "string"},
			"literalColor": map[string]any{
				"type": "string",
				"enum": []any{"red", "blue"},
			},
		},
		"required": []any{"allowedSecret", "allowedColor", "literalSecret", "literalColor"},
	})
	require.NoError(t, err)

	err = validateJSONAgainstSchema(
		[]byte(`{"allowedSecret":{"$bubuSecretRef":"ns/name:key"},"allowedColor":"{{ .inputs.color }}","literalSecret":"plain","literalColor":"red"}`), //nolint:lll
		schemaBytes,
		"test",
	)
	require.NoError(t, err)

	err = validateJSONAgainstSchema(
		[]byte(`{"allowedSecret":"plain","allowedColor":"red","literalSecret":{"$bubuSecretRef":"ns/name:key"},"literalColor":"{{ .inputs.color }}"}`), //nolint:lll
		schemaBytes,
		"test",
	)
	require.Error(t, err)
}

func TestValidateJSONAgainstSchema_IgnoresLegacyImplicitAlternativesEnv(t *testing.T) {
	t.Setenv("BUBU_SCHEMA_ALLOW_IMPLICIT_RUNTIME_ALTERNATIVES", "true")

	schemaBytes, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"secret": map[string]any{"type": "string"},
			"color": map[string]any{
				"type": "string",
				"enum": []any{"red", "blue"},
			},
		},
		"required": []any{"secret", "color"},
	})
	require.NoError(t, err)

	err = validateJSONAgainstSchema(
		[]byte(`{"secret":{"$bubuSecretRef":"ns/name:key"},"color":"{{ .inputs.color }}"}`),
		schemaBytes,
		"test",
	)
	require.Error(t, err)
}
