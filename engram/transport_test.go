package engram

import "testing"

func TestTransportDescriptorCloneCopiesTypedConfig(t *testing.T) {
	original := TransportDescriptor{
		Name: "rt",
		TypedConfig: &TransportConfig{
			TransportRef: "livekit-default",
			ModeReason:   "streaming-default",
		},
	}

	cloned := original.Clone()
	original.TypedConfig.TransportRef = "changed-ref"
	original.TypedConfig.ModeReason = "changed-reason"

	if cloned.TypedConfig == nil {
		t.Fatal("expected typed config clone")
	}
	if cloned.TypedConfig.TransportRef != "livekit-default" {
		t.Fatalf("expected cloned transport ref, got %q", cloned.TypedConfig.TransportRef)
	}
	if cloned.TypedConfig.ModeReason != "streaming-default" {
		t.Fatalf("expected cloned mode reason, got %q", cloned.TypedConfig.ModeReason)
	}
}
