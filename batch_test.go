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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/runtime"
)

func Test_bridgeEnabled_DefaultTrue(t *testing.T) {
	err := os.Unsetenv(contracts.HybridBridgeEnv)
	if err != nil {
		t.Fatalf("Unsetenv() error = %v", err)
	}
	assert.True(t, bridgeEnabled())
}

func Test_bridgeEnabled_Disabled(t *testing.T) {
	err := os.Setenv(contracts.HybridBridgeEnv, "false")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv(contracts.HybridBridgeEnv)
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	assert.False(t, bridgeEnabled())
}

func Test_getBridgeTimeout_Default(t *testing.T) {
	err := os.Unsetenv(contracts.HybridBridgeTimeoutEnv)
	if err != nil {
		t.Fatalf("Unsetenv() error = %v", err)
	}
	d := getBridgeTimeout()
	assert.Equal(t, 15*time.Second, d)
}

func Test_getBridgeTimeout_Override(t *testing.T) {
	err := os.Setenv(contracts.HybridBridgeTimeoutEnv, "123ms")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv(contracts.HybridBridgeTimeoutEnv)
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	assert.Equal(t, 123*time.Millisecond, getBridgeTimeout())
}

func Test_bridgeToHubSkipsWithoutBinding(t *testing.T) {
	t.Setenv(contracts.TransportBindingEnv, "")
	err := bridgeToHub(context.Background(), []byte(`{"ok":true}`), &runtime.ExecutionContextData{})
	require.NoError(t, err)
}

func Test_bridgeToHubDialFailure(t *testing.T) {
	t.Setenv(contracts.TransportBindingEnv, `{"binding":{"driver":"demo","endpoint":"dial-fail","protocolVersion":"`+coretransport.ProtocolVersion+`"}}`) //nolint:lll
	original := connectorDial
	connectorDial = func(
		ctx context.Context,
		endpoint string,
		env envResolver,
		opts ...grpc.DialOption,
	) (*TransportConnectorClient, error) {
		return nil, fmt.Errorf("dial error")
	}
	defer func() { connectorDial = original }()

	err := bridgeToHub(context.Background(), []byte(`{"ok":true}`), &runtime.ExecutionContextData{})
	require.Error(t, err)
}

type noopStorageManager struct{}

func (noopStorageManager) Hydrate(ctx context.Context, data any) (any, error) {
	return nil, nil
}

func (noopStorageManager) Dehydrate(ctx context.Context, data any, stepRunID string) (any, error) {
	return data, nil
}

type noopK8sClient struct{}

func (noopK8sClient) TriggerStory(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	return nil, nil
}

func (noopK8sClient) PatchStepRunStatus(
	ctx context.Context,
	stepRunName string,
	patchData runsv1alpha1.StepRunStatus,
) error {
	return nil
}

func TestHydrateAndUnmarshalInputs_AllowsNil(t *testing.T) {
	ctx := context.Background()
	execCtx := &runtime.ExecutionContextData{
		Inputs: make(map[string]any),
		StoryInfo: engram.StoryInfo{
			StepRunID: "step-run",
		},
	}
	_, err := hydrateAndUnmarshalInputs[struct{}, struct{}](ctx, noopStorageManager{}, noopK8sClient{}, execCtx)
	require.NoError(t, err)
}

func TestBuildHybridStreamMessage(t *testing.T) {
	execCtx := &runtime.ExecutionContextData{
		Inputs: map[string]any{"foo": "bar"},
		StoryInfo: engram.StoryInfo{
			StoryName:  "demo",
			StepRunID:  "step-123",
			StoryRunID: "run-9",
		},
		Transports: []engram.TransportDescriptor{
			{Name: "default", Kind: "live"},
		},
	}
	msg, err := buildHybridStreamMessage([]byte(`{"ok":true}`), execCtx)
	require.NoError(t, err)
	require.Equal(t, "data", msg.Kind)
	require.False(t, msg.Timestamp.IsZero())
	require.Equal(t, map[string]string{
		"storyName":  "demo",
		"stepRunID":  "step-123",
		"storyRunID": "run-9",
	}, msg.Metadata)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(msg.Inputs, &decoded))
	require.Equal(t, map[string]any{"foo": "bar"}, decoded)
	require.Len(t, msg.Transports, 1)
	execCtx.Transports[0].Name = "mutated"
	require.Equal(t, "default", msg.Transports[0].Name, "transports should be cloned")
}

func TestBuildHybridStreamMessageNilCtx(t *testing.T) {
	msg, err := buildHybridStreamMessage([]byte("x"), nil)
	require.NoError(t, err)
	require.Equal(t, "data", msg.Kind)
	require.False(t, msg.Timestamp.IsZero())
	require.Equal(t, []byte("x"), msg.Payload)
	require.Nil(t, msg.Metadata)
	require.Nil(t, msg.Transports)
	require.Nil(t, msg.Inputs)
}

func TestBuildHybridStreamMessageInputMarshalError(t *testing.T) {
	execCtx := &runtime.ExecutionContextData{
		Inputs: map[string]any{"bad": make(chan int)},
	}
	_, err := buildHybridStreamMessage([]byte("x"), execCtx)
	require.Error(t, err)
}

func TestTruncateErrorMessage(t *testing.T) {
	long := strings.Repeat("x", 10000)
	got := truncateErrorMessage(long, 8192)
	if len(got) > 8192 {
		t.Errorf("truncateErrorMessage should cap at 8192 bytes, got %d", len(got))
	}
	if len(got) != 8192 {
		t.Errorf("truncateErrorMessage should use exactly 8192 bytes for input longer than limit, got %d", len(got))
	}
}

func TestTruncateErrorMessage_ShortPassthrough(t *testing.T) {
	msg := "short error"
	got := truncateErrorMessage(msg, 8192)
	if got != msg {
		t.Errorf("truncateErrorMessage should return msg unchanged, got %q", got)
	}
}

func TestTruncateErrorMessage_ZeroLimit(t *testing.T) {
	msg := "some error"
	got := truncateErrorMessage(msg, 0)
	if got != msg {
		t.Errorf("zero limit should return msg unchanged, got %q", got)
	}
}
