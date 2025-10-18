package sdk

import (
	"testing"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
)

func TestAllowInsecureTransport(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.TransportSecurityModeEnv: contracts.TransportSecurityModePlaintext})
	if !allowInsecureTransport(env) {
		t.Fatalf("expected allowInsecureTransport to honor plaintext security mode")
	}
	env = newEnvResolver(map[string]string{contracts.TransportSecurityModeEnv: contracts.TransportSecurityModeTLS})
	if allowInsecureTransport(env) {
		t.Fatalf("expected allowInsecureTransport to enforce TLS security mode")
	}
}

func TestIsUnixEndpoint(t *testing.T) {
	if !isUnixEndpoint("unix:///tmp/connector.sock") {
		t.Fatalf("expected unix endpoint to be detected")
	}
	if isUnixEndpoint("tcp://127.0.0.1:9000") {
		t.Fatalf("expected tcp endpoint not to be treated as unix")
	}
}

func TestResolveDialTimeout(t *testing.T) {
	env := newEnvResolver(map[string]string{"BUBU_GRPC_DIAL_TIMEOUT": "3s"})
	if timeout := resolveDialTimeout(env); timeout != 3*time.Second {
		t.Fatalf("expected dial timeout 3s, got %s", timeout)
	}

	env = newEnvResolver(map[string]string{"BUBU_GRPC_DIAL_TIMEOUT": "invalid"})
	if timeout := resolveDialTimeout(env); timeout != defaultDialTimeout {
		t.Fatalf("expected fallback dial timeout %s, got %s", defaultDialTimeout, timeout)
	}
}
