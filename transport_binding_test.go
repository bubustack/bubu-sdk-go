package sdk

import (
	"encoding/json"
	"testing"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestParseTransportBindingEnvEnvelope(t *testing.T) {
	info := &transportpb.BindingInfo{
		Driver:   "demo",
		Endpoint: "connector:9000",
	}
	bindingJSON, err := protojson.Marshal(info)
	require.NoError(t, err)

	envPayload, err := json.Marshal(map[string]any{
		"name":      "binding-a",
		"namespace": "demo",
		"binding":   json.RawMessage(bindingJSON),
	})
	require.NoError(t, err)

	ref, err := parseTransportBindingEnv(string(envPayload))
	require.NoError(t, err)
	require.Equal(t, "binding-a", ref.Name)
	require.Equal(t, "demo", ref.Namespace)
	require.NotNil(t, ref.Info)
	require.Equal(t, "demo", ref.Info.GetDriver())
	require.Equal(t, "connector:9000", ref.Info.GetEndpoint())
}

func TestParseTransportBindingEnvJSONBindingInfo(t *testing.T) {
	info := &transportpb.BindingInfo{
		Driver:   "livekit",
		Endpoint: "unix:///tmp/connector.sock",
	}
	payload, err := protojson.Marshal(info)
	require.NoError(t, err)

	ref, err := parseTransportBindingEnv(string(payload))
	require.NoError(t, err)
	require.NotNil(t, ref.Info)
	require.Equal(t, "livekit", ref.Info.GetDriver())
	require.Equal(t, "unix:///tmp/connector.sock", ref.Info.GetEndpoint())
	require.Empty(t, ref.Name)
}

func TestParseTransportBindingEnvErrorsWithoutInlinePayload(t *testing.T) {
	_, err := parseTransportBindingEnv("binding-legacy")
	require.Error(t, err)
}
