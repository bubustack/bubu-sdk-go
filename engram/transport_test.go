package engram

import "testing"

func TestTransportDescriptorCloneDeepCopiesTypedConfigCollections(t *testing.T) {
	original := TransportDescriptor{
		Name: "rt",
		Kind: "livekit",
		Mode: "hot",
		Config: map[string]any{
			"labels": map[string]string{
				"room": "alpha",
			},
			"routes": []string{"primary", "backup"},
			"nested": []map[string]string{
				{"role": "writer"},
			},
		},
	}

	cloned := original.Clone()

	original.Config["labels"].(map[string]string)["room"] = "beta"
	original.Config["routes"].([]string)[0] = "mutated" //nolint:goconst
	original.Config["nested"].([]map[string]string)[0]["role"] = "reader"

	labels := cloned.Config["labels"].(map[string]string)
	if labels["room"] != "alpha" {
		t.Fatalf("expected typed map value to be cloned, got %q", labels["room"])
	}
	routes := cloned.Config["routes"].([]string)
	if routes[0] != "primary" {
		t.Fatalf("expected typed slice value to be cloned, got %q", routes[0])
	}
	nested := cloned.Config["nested"].([]map[string]string)
	if nested[0]["role"] != "writer" {
		t.Fatalf("expected nested typed collections to be cloned, got %q", nested[0]["role"])
	}
}

func TestTransportDescriptorCloneKeepsOriginalTypeShapes(t *testing.T) {
	original := TransportDescriptor{
		Config: map[string]any{
			"labels": map[string]string{"region": "us-east-1"},
			"routes": []string{"primary"},
		},
	}

	cloned := original.Clone()

	if _, ok := cloned.Config["labels"].(map[string]string); !ok {
		t.Fatalf("expected typed map to preserve its concrete type, got %T", cloned.Config["labels"])
	}
	if _, ok := cloned.Config["routes"].([]string); !ok {
		t.Fatalf("expected typed slice to preserve its concrete type, got %T", cloned.Config["routes"])
	}
}
