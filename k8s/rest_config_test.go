package k8s

import (
	"testing"
	"time"

	"github.com/bubustack/core/contracts"
	"k8s.io/client-go/rest"
)

func TestApplyDefaultRestConfigSettings_AppliesBaselineDefaults(t *testing.T) {
	cfg := &rest.Config{}

	applyDefaultRestConfigSettings(cfg)

	if cfg.QPS != 20 {
		t.Fatalf("QPS = %v, want 20", cfg.QPS)
	}
	if cfg.Burst != 40 {
		t.Fatalf("Burst = %d, want 40", cfg.Burst)
	}
	if cfg.Timeout != 30*time.Second {
		t.Fatalf("Timeout = %s, want 30s", cfg.Timeout)
	}
}

func TestApplyEnvOverridesToRestConfig_OverridesTimeoutAndUserAgent(t *testing.T) {
	t.Setenv(contracts.K8sTimeoutEnv, "45s")
	t.Setenv(contracts.K8sUserAgentEnv, "custom-agent")

	cfg := &rest.Config{
		QPS:       33,
		Burst:     66,
		Timeout:   5 * time.Second,
		UserAgent: "old-agent",
	}

	applyEnvOverridesToRestConfig(cfg)

	if cfg.Timeout != 45*time.Second {
		t.Fatalf("Timeout = %s, want 45s", cfg.Timeout)
	}
	if cfg.UserAgent != "custom-agent" {
		t.Fatalf("UserAgent = %q, want %q", cfg.UserAgent, "custom-agent")
	}
	if cfg.QPS != 33 {
		t.Fatalf("QPS = %v, want 33", cfg.QPS)
	}
	if cfg.Burst != 66 {
		t.Fatalf("Burst = %d, want 66", cfg.Burst)
	}
}

func TestApplyEnvOverridesToRestConfig_IgnoresInvalidTimeoutAndKeepsDefaultUserAgent(t *testing.T) {
	t.Setenv(contracts.K8sTimeoutEnv, "not-a-duration")
	t.Setenv(contracts.K8sUserAgentEnv, "")

	cfg := &rest.Config{
		Timeout:   12 * time.Second,
		UserAgent: "original-agent",
	}

	applyEnvOverridesToRestConfig(cfg)

	if cfg.Timeout != 12*time.Second {
		t.Fatalf("Timeout = %s, want 12s", cfg.Timeout)
	}
	if cfg.UserAgent != "bubu-sdk-go" { //nolint:goconst
		t.Fatalf("UserAgent = %q, want %q", cfg.UserAgent, "bubu-sdk-go")
	}
}
