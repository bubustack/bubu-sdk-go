package sdk

import (
	"encoding/json"
	"testing"

	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestBindingReferenceFromEnvEnvelope(t *testing.T) {
	info := &transportpb.BindingInfo{
		Driver:          "demo",
		Endpoint:        "connector:9000",
		ProtocolVersion: coretransport.ProtocolVersion,
	}
	bindingJSON, err := protojson.Marshal(info)
	require.NoError(t, err)

	envPayload, err := json.Marshal(map[string]any{
		"name":      "binding-a",
		"namespace": "demo",
		"binding":   json.RawMessage(bindingJSON),
	})
	require.NoError(t, err)

	t.Setenv(contracts.TransportBindingEnv, string(envPayload))
	ref, err := bindingReferenceFromEnv()
	require.NoError(t, err)
	require.Equal(t, "binding-a", ref.Name)
	require.Equal(t, "demo", ref.Namespace)
	require.NotNil(t, ref.Info)
	require.Equal(t, "demo", ref.Info.GetDriver())
	require.Equal(t, "connector:9000", ref.Info.GetEndpoint())
}

func TestBindingReferenceFromEnvRejectsBareBindingInfoPayload(t *testing.T) {
	info := &transportpb.BindingInfo{
		Driver:          "livekit",
		Endpoint:        "unix:///tmp/connector.sock",
		ProtocolVersion: coretransport.ProtocolVersion,
	}
	payload, err := protojson.Marshal(info)
	require.NoError(t, err)

	t.Setenv(contracts.TransportBindingEnv, string(payload))
	_, err = bindingReferenceFromEnv()
	require.ErrorContains(t, err, "binding envelope name is required")
}

func TestBindingReferenceFromEnvErrorsWithoutInlinePayload(t *testing.T) {
	t.Setenv(contracts.TransportBindingEnv, "binding-legacy")
	_, err := bindingReferenceFromEnv()
	require.Error(t, err)
}

func TestBindingReferenceFromEnvRejectsProtocolVersionMismatch(t *testing.T) {
	info := &transportpb.BindingInfo{
		Driver:          "demo",
		Endpoint:        "connector:9000",
		ProtocolVersion: "9.9.9",
	}
	bindingJSON, err := protojson.Marshal(info)
	require.NoError(t, err)

	envPayload, err := json.Marshal(map[string]any{
		"name":      "binding-a",
		"namespace": "demo",
		"binding":   json.RawMessage(bindingJSON),
	})
	require.NoError(t, err)

	t.Setenv(contracts.TransportBindingEnv, string(envPayload))
	_, err = bindingReferenceFromEnv()
	require.ErrorContains(t, err, "invalid transport protocol version in binding")
	require.ErrorContains(t, err, "unsupported transport protocol version")
}
