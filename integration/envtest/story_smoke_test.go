//go:build integration

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
package envtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bubu-sdk-go/k8s"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func resolveCRDPath(t *testing.T) string {
	t.Helper()

	if override := os.Getenv("BOBRAPET_CRD_PATH"); override != "" {
		if info, err := os.Stat(override); err == nil && info.IsDir() {
			return override
		}
		t.Fatalf("BOBRAPET_CRD_PATH=%q does not exist or is not a directory", override)
	}

	candidates := []string{
		filepath.Join("..", "..", "bobrapet", "config", "crd", "bases"),
		filepath.Join("..", "..", "..", "bobrapet", "config", "crd", "bases"),
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
	}

	t.Skip("bobrapet CRDs not found; set BOBRAPET_CRD_PATH or run tests within the bobrapet+bubu-sdk-go workspace")
	return ""
}

func TestStorySubmissionSmoke(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest smoke test")
	}

	crdPath := resolveCRDPath(t)
	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{crdPath},
	}

	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnv.Stop())
	})

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))

	apiClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	const namespace = "default"
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "smoke-story",
			Namespace: namespace,
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "start",
				Type: enums.StepTypeSetData,
			}},
		},
	}
	require.NoError(t, apiClient.Create(context.Background(), story))

	t.Setenv(contracts.TargetStoryNamespaceEnv, namespace)

	sdkClient, err := k8s.NewClientWithConfig(cfg)
	require.NoError(t, err)

	run, err := sdkClient.TriggerStory(context.Background(), "smoke-story", namespace, map[string]any{"hello": "world"})
	require.NoError(t, err)
	require.NotNil(t, run)

	fetched := &runsv1alpha1.StoryRun{}
	require.NoError(t, apiClient.Get(context.Background(), client.ObjectKey{Name: run.Name, Namespace: namespace}, fetched))
	require.Equal(t, "smoke-story", fetched.Spec.StoryRef.Name)
	require.NotNil(t, fetched.Spec.Inputs)
	require.Contains(t, string(fetched.Spec.Inputs.Raw), "hello")
}
