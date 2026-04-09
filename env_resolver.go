package sdk

import (
	"os"
	"strings"
)

// envResolver merges binding-scoped overrides with process environment lookups.
type envResolver struct {
	overrides map[string]string
}

func newEnvResolver(overrides map[string]string) envResolver {
	if len(overrides) == 0 {
		return envResolver{}
	}
	clean := make(map[string]string, len(overrides))
	for key, value := range overrides {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		clean[name] = strings.TrimSpace(value)
	}
	return envResolver{overrides: clean}
}

func (r envResolver) lookup(key string) string {
	if r.overrides != nil {
		if value, ok := r.overrides[key]; ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return os.Getenv(key)
}

func (r envResolver) Lookup(key string) string {
	return r.lookup(key)
}
