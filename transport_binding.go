package sdk

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"

	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

var errBindingEnvMissing = errors.New("BUBU_TRANSPORT_BINDING not set")

type bindingReference struct {
	Name      string
	Namespace string
	Raw       string
	Info      *transportpb.BindingInfo
}

func bindingReferenceFromEnv() (bindingReference, error) {
	value := strings.TrimSpace(os.Getenv(contracts.TransportBindingEnv))
	if value == "" {
		return bindingReference{}, errBindingEnvMissing
	}
	payload, err := coretransport.ParseBindingPayload(value)
	if err != nil {
		return bindingReference{}, err
	}
	ref := bindingReference{
		Name:      payload.Reference.Name,
		Namespace: payload.Reference.Namespace,
		Info:      payload.Info,
		Raw:       payload.Raw,
	}
	if err := validateBindingInfoProtocol(ref.Info); err != nil {
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

func validateBindingInfoProtocol(info *transportpb.BindingInfo) error {
	if info == nil {
		return nil
	}
	if err := coretransport.ValidateProtocolVersion(info.GetProtocolVersion()); err != nil {
		return fmt.Errorf("invalid transport protocol version in binding: %w", err)
	}
	return nil
}

func (ref bindingReference) endpoint() string {
	if ref.Info == nil {
		return ""
	}
	return strings.TrimSpace(ref.Info.GetEndpoint())
}

func (ref bindingReference) envOverrides() map[string]string {
	overrides := coretransport.BindingEnvOverrides(ref.Info)
	if len(overrides) == 0 {
		return nil
	}
	copyMap := make(map[string]string, len(overrides))
	maps.Copy(copyMap, overrides)
	return copyMap
}
