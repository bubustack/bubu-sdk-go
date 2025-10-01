package util

import "encoding/json"

// DeepCopyMap creates a deep copy of a map[string]interface{} by marshaling
// and unmarshaling it through JSON. This is a convenient way to create a mutable
// copy of complex nested structures.
func DeepCopyMap(original map[string]interface{}) (map[string]interface{}, error) {
	newMap := make(map[string]interface{})
	bytes, err := json.Marshal(original)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &newMap)
	if err != nil {
		return nil, err
	}
	return newMap, nil
}
