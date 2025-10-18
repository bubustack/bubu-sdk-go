package sdk

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_bridgeEnabled_DefaultTrue(t *testing.T) {
	err := os.Unsetenv("BUBU_HYBRID_BRIDGE")
	if err != nil {
		t.Fatalf("Unsetenv() error = %v", err)
	}
	assert.True(t, bridgeEnabled())
}

func Test_bridgeEnabled_Disabled(t *testing.T) {
	err := os.Setenv("BUBU_HYBRID_BRIDGE", "false")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv("BUBU_HYBRID_BRIDGE")
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	assert.False(t, bridgeEnabled())
}

func Test_getBridgeTimeout_Default(t *testing.T) {
	err := os.Unsetenv("BUBU_HYBRID_BRIDGE_TIMEOUT")
	if err != nil {
		t.Fatalf("Unsetenv() error = %v", err)
	}
	d := getBridgeTimeout()
	assert.Equal(t, 15*time.Second, d)
}

func Test_getBridgeTimeout_Override(t *testing.T) {
	err := os.Setenv("BUBU_HYBRID_BRIDGE_TIMEOUT", "123ms")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv("BUBU_HYBRID_BRIDGE_TIMEOUT")
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	assert.Equal(t, 123*time.Millisecond, getBridgeTimeout())
}

func Test_getHubTarget_EnvOverride(t *testing.T) {
	err := os.Setenv("DOWNSTREAM_HOST", "example:9000")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv("DOWNSTREAM_HOST")
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	assert.Equal(t, "example:9000", getHubTarget())
}
