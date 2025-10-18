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

package observability

import (
	"os"
	"sync"
	"testing"

	"github.com/bubustack/bobrapet/pkg/contracts"
)

func resetForTest(t *testing.T) {
	t.Helper()
	initOnce = sync.Once{}
}

func TestMetricsEnabledFromEnv(t *testing.T) {
	t.Setenv(contracts.SDKMetricsEnabledEnv, "false")
	resetForTest(t)
	if MetricsEnabled() {
		t.Fatalf("expected metrics disabled via env")
	}
}

func TestTracingEnabledDefault(t *testing.T) {
	_ = os.Unsetenv(contracts.SDKTracingEnabledEnv)
	resetForTest(t)
	if !TracingEnabled() {
		t.Fatalf("expected tracing enabled by default")
	}
}
