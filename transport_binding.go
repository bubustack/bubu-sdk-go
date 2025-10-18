package sdk

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bobrapet/pkg/transport/bindinginfo"
	"github.com/bubustack/bubu-sdk-go/k8s"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var errBindingEnvMissing = errors.New("BUBU_TRANSPORT_BINDING not set")

type bindingReference struct {
	Name      string
	Namespace string
	Raw       string
	Info      *transportpb.BindingInfo
}

type bindingEnvelope struct {
	Name      string          `json:"name"`
	Namespace string          `json:"namespace,omitempty"`
	Binding   json.RawMessage `json:"binding"`
	Info      json.RawMessage `json:"info"`
}

func parseTransportBindingEnv(value string) (bindingReference, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return bindingReference{}, fmt.Errorf("empty transport binding env")
	}

	// Attempt to parse as JSON envelope or direct BindingInfo JSON.
	if strings.HasPrefix(value, "{") {
		if ref, err := parseBindingJSON([]byte(value)); err == nil {
			ref.Raw = value
			return ref, nil
		}
	}

	// Attempt base64-decoded proto binary.
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
		if info, derr := decodeBindingInfoBinary(decoded); derr == nil {
			return bindingReference{Info: info, Raw: value}, nil
		}
	}

	return bindingReference{}, fmt.Errorf("inline binding payload not detected")
}

func parseBindingJSON(data []byte) (bindingReference, error) {
	var env bindingEnvelope
	if err := json.Unmarshal(data, &env); err == nil {
		if ref, err := buildReferenceFromEnvelope(env); err == nil {
			return ref, nil
		}
	}

	info, err := decodeBindingInfoJSON(data)
	if err != nil {
		return bindingReference{}, err
	}
	return bindingReference{Info: info}, nil
}

func buildReferenceFromEnvelope(env bindingEnvelope) (bindingReference, error) {
	var blob []byte
	switch {
	case len(env.Binding) > 0:
		blob = env.Binding
	case len(env.Info) > 0:
		blob = env.Info
	default:
		return bindingReference{}, fmt.Errorf("binding envelope missing info payload")
	}
	info, err := decodeBindingInfoJSON(blob)
	if err != nil {
		return bindingReference{}, err
	}
	return bindingReference{
		Name:      strings.TrimSpace(env.Name),
		Namespace: strings.TrimSpace(env.Namespace),
		Info:      info,
	}, nil
}

func decodeBindingInfoJSON(data []byte) (*transportpb.BindingInfo, error) {
	var info transportpb.BindingInfo
	if err := protojson.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func decodeBindingInfoBinary(data []byte) (*transportpb.BindingInfo, error) {
	var info transportpb.BindingInfo
	if err := proto.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func bindingReferenceFromEnv() (bindingReference, error) {
	value := strings.TrimSpace(os.Getenv(contracts.TransportBindingEnv))
	if value == "" {
		return bindingReference{}, errBindingEnvMissing
	}
	ref, err := parseTransportBindingEnv(value)
	if err != nil {
		return bindingReference{}, err
	}
	if ref.Namespace == "" {
		ref.Namespace = k8s.ResolvePodNamespace()
	}
	if ref.Raw == "" {
		ref.Raw = value
	}
	return ref, nil
}

func (ref bindingReference) endpoint() string {
	if ref.Info == nil {
		return ""
	}
	return strings.TrimSpace(ref.Info.GetEndpoint())
}

func (ref bindingReference) envOverrides() map[string]string {
	if ref.Info == nil {
		return nil
	}
	overrides := bindinginfo.EnvOverrides(ref.Info)
	if len(overrides) == 0 {
		return nil
	}
	copyMap := make(map[string]string, len(overrides))
	for key, value := range overrides {
		copyMap[key] = value
	}
	return copyMap
}
