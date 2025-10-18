package engram

// TransportDescriptor describes a named transport binding declared on the Story.
// Config carries arbitrary transport-specific settings (e.g. livekit/storage blocks).
type TransportDescriptor struct {
	Name   string         `json:"name"`
	Kind   string         `json:"kind"`
	Mode   string         `json:"mode,omitempty"`
	Config map[string]any `json:"config,omitempty"`
}

// Clone returns a deep copy of the descriptor to avoid callers mutating shared state.
func (t TransportDescriptor) Clone() TransportDescriptor {
	clone := t
	if t.Config != nil {
		clone.Config = cloneConfigMap(t.Config)
	}
	return clone
}

func cloneConfigMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = cloneConfigValue(v)
	}
	return out
}

func cloneConfigValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneConfigMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i := range typed {
			out[i] = cloneConfigValue(typed[i])
		}
		return out
	default:
		return typed
	}
}
