//go:build go1.18

/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
			Key      string         `mapstructure:"key"`
			Number   int            `mapstructure:"number"`
			Float    float64        `mapstructure:"float"`
			Bool     bool           `mapstructure:"bool"`
			Array    []int          `mapstructure:"array"`
			Nested   map[string]any `mapstructure:"nested"`
			Duration time.Duration  `mapstructure:"duration"`
			Timeout  time.Duration  `mapstructure:"timeout"`
			Deeply   map[string]any `mapstructure:"deeply"`
		}

		// Should not panic, even with malformed data
		_, err := UnmarshalFromMap[TestStruct](data)
		_ = err // Error is acceptable, but no panic
	})
}
