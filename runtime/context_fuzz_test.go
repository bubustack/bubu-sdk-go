//go:build go1.18
// +build go1.18

package runtime

import (
	"encoding/json"
	"testing"
	"time"
)

func FuzzUnmarshalFromMap(f *testing.F) {
	// Seed corpus with common patterns
	f.Add(`{"key":"value","nested":{"inner":"data"}}`)
	f.Add(`{"number":123,"float":3.14,"bool":true}`)
	f.Add(`{"array":[1,2,3],"empty":{}}`)
	f.Add(`{"duration":"5s","timeout":"1m30s"}`)
	f.Add(`{"deeply":{"nested":{"structure":{"with":{"many":{"levels":"data"}}}}}}`)

	f.Fuzz(func(t *testing.T, jsonStr string) {
		// Parse JSON into map
		var data map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			t.Skip("invalid JSON")
		}

		// Try to unmarshal into a generic struct
		type TestStruct struct {
			Key      string                 `mapstructure:"key"`
			Number   int                    `mapstructure:"number"`
			Float    float64                `mapstructure:"float"`
			Bool     bool                   `mapstructure:"bool"`
			Array    []int                  `mapstructure:"array"`
			Nested   map[string]interface{} `mapstructure:"nested"`
			Duration time.Duration          `mapstructure:"duration"`
			Timeout  time.Duration          `mapstructure:"timeout"`
			Deeply   map[string]interface{} `mapstructure:"deeply"`
		}

		// Should not panic, even with malformed data
		_, err := UnmarshalFromMap[TestStruct](data)
		_ = err // Error is acceptable, but no panic
	})
}
