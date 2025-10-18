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

package env

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// GetWithFallback gets a trimmed environment variable, trying the primary key first and then the fallback key.
func GetWithFallback(primaryKey, fallbackKey string) string {
	if val := strings.TrimSpace(os.Getenv(primaryKey)); val != "" {
		return val
	}
	return strings.TrimSpace(os.Getenv(fallbackKey))
}

// GetDurationWithFallback parses a duration from an environment variable, with a fallback key and a default value.
func GetDurationWithFallback(primaryKey, fallbackKey string, defaultValue time.Duration) time.Duration {
	valStr := GetWithFallback(primaryKey, fallbackKey)
	if val, err := time.ParseDuration(valStr); err == nil && val > 0 {
		return val
	}
	return defaultValue
}

// GetIntWithFallback parses an integer from an environment variable, with a fallback key and a default value.
func GetIntWithFallback(primaryKey, fallbackKey string, defaultValue int) int {
	valStr := GetWithFallback(primaryKey, fallbackKey)
	if val, err := strconv.Atoi(valStr); err == nil && val > 0 {
		return val
	}
	return defaultValue
}

// GetEnvWithFallback gets an environment variable by key, returning a fallback if not set.
func GetEnvWithFallback(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
