package sdk

import (
	"encoding/json"
	"strings"

	"github.com/bubustack/bubu-sdk-go/engram"
)

func cloneTransportDescriptors(src []engram.TransportDescriptor) []engram.TransportDescriptor {
	if len(src) == 0 {
		return nil
	}
	out := make([]engram.TransportDescriptor, len(src))
	for i := range src {
		out[i] = src[i].Clone()
	}
	return out
}

func storyMetadata(info engram.StoryInfo) map[string]string {
	meta := make(map[string]string, 5)
	if v := strings.TrimSpace(info.StoryName); v != "" {
		meta["storyName"] = v
	}
	if v := strings.TrimSpace(info.StoryRunID); v != "" {
		meta["storyRunID"] = v
	}
	if v := strings.TrimSpace(info.StepName); v != "" {
		meta["stepName"] = v
	}
	if v := strings.TrimSpace(info.StepRunID); v != "" {
		meta["stepRunID"] = v
	}
	if v := strings.TrimSpace(info.StepRunNamespace); v != "" {
		meta["stepRunNamespace"] = v
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

func inputsJSON(inputs map[string]any) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	payload, err := json.Marshal(inputs)
	if err != nil {
		return nil, err
	}
	return payload, nil
}
