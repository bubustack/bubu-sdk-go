package sdk

import (
	"testing"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
)

func TestResolveChannelBufferSizeFromEnv(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCChannelBufferSizeEnv: "64"})
	if size := resolveChannelBufferSize(env); size != 64 {
		t.Fatalf("expected buffer size 64, got %d", size)
	}
}

func TestResolveChannelBufferSizeDefault(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCChannelBufferSizeEnv: "invalid"})
	if size := resolveChannelBufferSize(env); size != DefaultChannelBufferSize {
		t.Fatalf("expected default buffer size %d, got %d", DefaultChannelBufferSize, size)
	}
}

func TestResolveReconnectPolicyDefaults(t *testing.T) {
	env := newEnvResolver(nil)
	policy := resolveReconnectPolicy(env)
	if policy.base != defaultReconnectBaseBackoff {
		t.Fatalf("expected base backoff %s, got %s", defaultReconnectBaseBackoff, policy.base)
	}
	if policy.max != defaultReconnectMaxBackoff {
		t.Fatalf("expected max backoff %s, got %s", defaultReconnectMaxBackoff, policy.max)
	}
	if policy.maxRetries != defaultReconnectMaxRetries {
		t.Fatalf("expected max retries %d, got %d", defaultReconnectMaxRetries, policy.maxRetries)
	}
}

func TestResolveReconnectPolicyFromEnv(t *testing.T) {
	env := newEnvResolver(map[string]string{
		contracts.GRPCReconnectBaseBackoffEnv: "750ms",
		contracts.GRPCReconnectMaxBackoffEnv:  "45s",
		contracts.GRPCReconnectMaxRetriesEnv:  "5",
	})

	policy := resolveReconnectPolicy(env)
	if policy.base != 750*time.Millisecond {
		t.Fatalf("expected base backoff 750ms, got %s", policy.base)
	}
	if policy.max != 45*time.Second {
		t.Fatalf("expected max backoff 45s, got %s", policy.max)
	}
	if policy.maxRetries != 5 {
		t.Fatalf("expected max retries 5, got %d", policy.maxRetries)
	}
}

func TestResolveMessageTimeoutDefault(t *testing.T) {
	env := newEnvResolver(nil)
	if timeout := resolveMessageTimeout(env); timeout != defaultMessageTimeout {
		t.Fatalf("expected default message timeout %s, got %s", defaultMessageTimeout, timeout)
	}
}

func TestResolveMessageTimeoutOverride(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCMessageTimeoutEnv: "45s"})
	if timeout := resolveMessageTimeout(env); timeout != 45*time.Second {
		t.Fatalf("expected override timeout 45s, got %s", timeout)
	}
}

func TestResolveChannelSendTimeout(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCChannelSendTimeoutEnv: "5s"})
	if timeout := resolveChannelSendTimeout(env); timeout != 5*time.Second {
		t.Fatalf("expected channel send timeout 5s, got %s", timeout)
	}
}

func TestResolveHangTimeout(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCHangTimeoutEnv: "0"})
	if timeout := resolveHangTimeout(env); timeout != 0 {
		t.Fatalf("expected zero timeout when env is 0, got %s", timeout)
	}
	env = newEnvResolver(map[string]string{contracts.GRPCHangTimeoutEnv: "20s"})
	if timeout := resolveHangTimeout(env); timeout != 20*time.Second {
		t.Fatalf("expected hang timeout 20s, got %s", timeout)
	}
}
