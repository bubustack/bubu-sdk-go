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

package sdk

import (
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/engram"
)

type manifestTestPayload struct {
	Summary string                 `json:"summary"`
	Items   []manifestTestItem     `json:"items"`
	Meta    *manifestNestedPayload `json:"meta,omitempty"`
}

type manifestTestItem struct {
	ID string `json:"id"`
}

type manifestNestedPayload struct {
	Status string `json:"status"`
}

func TestBuildManifestDataSupportsStructPayloads(t *testing.T) {
	payload := manifestTestPayload{
		Summary: "done",
		Items:   []manifestTestItem{{ID: "a"}},
		Meta:    &manifestNestedPayload{Status: "ok"},
	}
	result := engram.NewResultFrom(payload)

	requests := []runsv1alpha1.ManifestRequest{
		{Path: "summary", Operations: []runsv1alpha1.ManifestOperation{runsv1alpha1.ManifestOperationExists}},
		{
			Path: "items",
			Operations: []runsv1alpha1.ManifestOperation{
				runsv1alpha1.ManifestOperationExists,
				runsv1alpha1.ManifestOperationLength,
			},
		},
		{Path: "items[0].id", Operations: []runsv1alpha1.ManifestOperation{runsv1alpha1.ManifestOperationExists}},
		{Path: "meta.status", Operations: []runsv1alpha1.ManifestOperation{runsv1alpha1.ManifestOperationExists}},
	}

	manifest, warnings := buildManifestData(result, requests)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}

	assertExists(t, manifest, "summary", true)
	assertExists(t, manifest, "items[0].id", true)

	itemsEntry, ok := manifest["items"]
	if !ok {
		t.Fatalf("expected manifest entry for items")
	}
	if itemsEntry.Length == nil || *itemsEntry.Length != 1 {
		t.Fatalf("expected items length to be 1, got %+v", itemsEntry.Length)
	}
}

func TestBuildManifestDataHandlesPointerPayloads(t *testing.T) {
	payload := &manifestTestPayload{
		Summary: "pointer",
		Items:   []manifestTestItem{},
	}

	manifest, warnings := buildManifestData(engram.NewResultFrom(payload), []runsv1alpha1.ManifestRequest{
		{Path: "summary", Operations: []runsv1alpha1.ManifestOperation{runsv1alpha1.ManifestOperationExists}},
	})

	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}
	assertExists(t, manifest, "summary", true)
}

func assertExists(t *testing.T, manifest map[string]runsv1alpha1.StepManifestData, key string, expected bool) {
	t.Helper()
	entry, ok := manifest[key]
	if !ok {
		t.Fatalf("expected manifest entry for %q", key)
	}
	if entry.Exists == nil || *entry.Exists != expected {
		t.Fatalf("expected manifest Exists=%t for %q, got %+v", expected, key, entry.Exists)
	}
	if entry.Error != "" {
		t.Fatalf("expected no error for %q, got %q", key, entry.Error)
	}
}
