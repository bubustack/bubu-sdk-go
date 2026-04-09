package sdk

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bubustack/core/contracts"
	transportconnector "github.com/bubustack/core/runtime/transport/connector"
	"google.golang.org/grpc"
)

func TestValidateTransportSecurityMode(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.TransportSecurityModeEnv: "plaintext"})
	if err := validateTransportSecurityMode(env); err == nil {
		t.Fatalf("expected plaintext security mode to be rejected")
	}
	env = newEnvResolver(map[string]string{contracts.TransportSecurityModeEnv: contracts.TransportSecurityModeTLS})
	if err := validateTransportSecurityMode(env); err != nil {
		t.Fatalf("expected tls security mode to be accepted, got %v", err)
	}
	env = newEnvResolver(nil)
	if err := validateTransportSecurityMode(env); err != nil {
		t.Fatalf("expected default security mode to be accepted, got %v", err)
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
	if timeout := transportconnector.DialTimeout(env, defaultDialTimeout); timeout != 3*time.Second {
		t.Fatalf("expected dial timeout 3s, got %s", timeout)
	}

	env = newEnvResolver(map[string]string{"BUBU_GRPC_DIAL_TIMEOUT": "invalid"})
	if timeout := transportconnector.DialTimeout(env, defaultDialTimeout); timeout != defaultDialTimeout {
		t.Fatalf("expected fallback dial timeout %s, got %s", defaultDialTimeout, timeout)
	}
}

func TestDialTransportConnectorUsesPlaintextForLoopbackEndpoints(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	defer server.Stop()
	go func() {
		_ = server.Serve(listener)
	}()

	env := newEnvResolver(map[string]string{
		contracts.TransportSecurityModeEnv: contracts.TransportSecurityModeTLS,
		contracts.GRPCDialTimeoutEnv:       "1s",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := dialTransportConnector(ctx, listener.Addr().String(), env)
	if err != nil {
		t.Fatalf("dialTransportConnector: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()
}
