package engram

import "reflect"

// TransportDescriptor describes a named transport binding declared on the Story.
// Config carries arbitrary transport-specific settings (e.g. livekit/storage blocks).
type TransportDescriptor struct {
	// Name is the transport binding name referenced by the Story.
	Name string `json:"name"`
	// Kind identifies the transport driver kind, such as "livekit" or "storage".
	Kind string `json:"kind"`
	// Mode selects the runtime behavior for the transport binding.
	Mode string `json:"mode,omitempty"`
	// Config carries arbitrary transport-specific settings.
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
	if value == nil {
		return nil
	}
	return cloneConfigReflectValue(reflect.ValueOf(value)).Interface()
}

func cloneConfigReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneConfigReflectValue(value.Elem())
		out := reflect.New(value.Type()).Elem()
		out.Set(cloned)
		return out
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(cloneConfigReflectValue(value.Elem()))
		return out
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), cloneConfigReflectValue(iter.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(cloneConfigReflectValue(value.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(cloneConfigReflectValue(value.Index(i)))
		}
		return out
	default:
		return value
	}
}
