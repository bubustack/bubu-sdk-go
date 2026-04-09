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

// Package env provides small helpers for parsing validated environment
// overrides used across the SDK runtime.
package env

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// GetDuration parses a duration from an environment variable, returning defaultValue if unset or invalid.
func GetDuration(key string, defaultValue time.Duration) time.Duration {
	valStr := strings.TrimSpace(os.Getenv(key))
	if valStr == "" {
		return defaultValue
	}
	val, err := time.ParseDuration(valStr)
	if err != nil || val <= 0 {
		slog.Warn("ignoring invalid env var duration, using default",
			"key", key, "value", valStr, "default", defaultValue)
		return defaultValue
	}
	return val
}

// GetInt parses an integer from an environment variable, returning defaultValue if unset or invalid.
func GetInt(key string, defaultValue int) int {
	valStr := strings.TrimSpace(os.Getenv(key))
	if valStr == "" {
		return defaultValue
	}
	val, err := strconv.Atoi(valStr)
	if err != nil || val <= 0 {
		slog.Warn("ignoring invalid env var integer, using default",
			"key", key, "value", valStr, "default", defaultValue)
		return defaultValue
	}
	return val
}
