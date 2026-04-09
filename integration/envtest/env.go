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
	"os"
	"path/filepath"
	"testing"
	"time"

	bubuk8s "github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlenvtest "sigs.k8s.io/controller-runtime/pkg/envtest"
)

type sdkEnvtestHarness struct { //nolint:unused
	apiClient client.Client
	sdkClient *bubuk8s.Client
	scheme    *runtime.Scheme
	namespace string
}

const (
	triggerResolverPollIntervalEnv   = "BUBU_ENVTEST_TRIGGER_RESOLVER_POLL_INTERVAL"   //nolint:unused
	triggerResolverRequestTimeoutEnv = "BUBU_ENVTEST_TRIGGER_RESOLVER_REQUEST_TIMEOUT" //nolint:unused
	triggerResolverStopTimeoutEnv    = "BUBU_ENVTEST_TRIGGER_RESOLVER_STOP_TIMEOUT"    //nolint:unused
)

func triggerResolverPollInterval() time.Duration { //nolint:unused
	return parsePositiveDurationEnv(triggerResolverPollIntervalEnv, 20*time.Millisecond)
}

func triggerResolverRequestTimeout() time.Duration { //nolint:unused
	return parsePositiveDurationEnv(triggerResolverRequestTimeoutEnv, 500*time.Millisecond)
}

func triggerResolverStopTimeout() time.Duration { //nolint:unused
	return parsePositiveDurationEnv(triggerResolverStopTimeoutEnv, 2*time.Second)
}

func parsePositiveDurationEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func waitForSignal(done <-chan struct{}, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func resolveCRDPath(t *testing.T) string { //nolint:unused
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

func setupSDKEnvtest(t *testing.T, addToSchemes ...func(*runtime.Scheme) error) *sdkEnvtestHarness { //nolint:unused
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration envtest in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest smoke test")
	}

	testEnv := &ctrlenvtest.Environment{
		CRDDirectoryPaths: []string{resolveCRDPath(t)},
	}

	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnv.Stop())
	})

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	for _, addToScheme := range addToSchemes {
		require.NoError(t, addToScheme(scheme))
	}

	apiClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	const namespace = "default"
	t.Setenv(contracts.TargetStoryNamespaceEnv, namespace)

	sdkClient, err := bubuk8s.NewClientForConfig(cfg)
	require.NoError(t, err)

	return &sdkEnvtestHarness{
		apiClient: apiClient,
		sdkClient: sdkClient,
		scheme:    scheme,
		namespace: namespace,
	}
}
