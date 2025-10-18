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
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"strings"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bubu-sdk-go/k8s"
)

type storyRuntime struct {
	start func(
		ctx context.Context,
		storyName string,
		storyNamespace string,
		inputs map[string]any,
	) (*runsv1alpha1.StoryRun, error)
	stop func(ctx context.Context, storyRunName, storyNamespace string) error
}

// StoryDispatcherOption configures a StoryDispatcher.
type StoryDispatcherOption func(*StoryDispatcher)

// WithStoryRuntime overrides the start/stop implementation used by the dispatcher.
// Intended primarily for tests.
func WithStoryRuntime(
	start func(
		ctx context.Context,
		storyName string,
		storyNamespace string,
		inputs map[string]any,
	) (*runsv1alpha1.StoryRun, error),
	stop func(ctx context.Context, storyRunName, storyNamespace string) error,
) StoryDispatcherOption {
	return func(d *StoryDispatcher) {
		d.runtime = storyRuntime{start: start, stop: stop}
	}
}

// StorySession holds metadata about an active StoryRun started by an impulse.
type StorySession struct {
	Key       string
	StoryRun  string
	Namespace string
	StoryName string
	StartedAt time.Time
	Metadata  map[string]string
}

// StoryTriggerRequest defines the inputs required to trigger a story.
type StoryTriggerRequest struct {
	Key            string
	StoryName      string
	StoryNamespace string
	Inputs         map[string]any
	Metadata       map[string]string
}

// StoryTriggerResult returns the StoryRun created by the dispatcher and the associated session.
type StoryTriggerResult struct {
	StoryRun *runsv1alpha1.StoryRun
	Session  *StorySession
}

// StoryDispatcher manages StoryRun lifecycles on behalf of an impulse, providing
// session tracking and idempotent stop semantics.
type StoryDispatcher struct {
	mu               sync.RWMutex
	sessions         map[string]*StorySession
	runtime          storyRuntime
	timeSource       func() time.Time
	statsClient      *k8s.Client
	impulseName      string
	impulseNamespace string
}

// NewStoryDispatcher creates a StoryDispatcher with optional configuration.
func NewStoryDispatcher(opts ...StoryDispatcherOption) *StoryDispatcher {
	d := &StoryDispatcher{
		sessions: make(map[string]*StorySession),
		runtime: storyRuntime{
			start: StartStoryInNamespace,
			stop:  StopStoryInNamespace,
		},
		timeSource: time.Now,
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.runtime.start == nil {
		d.runtime.start = StartStoryInNamespace
	}
	if d.runtime.stop == nil {
		d.runtime.stop = StopStoryInNamespace
	}
	if d.timeSource == nil {
		d.timeSource = time.Now
	}
	d.initImpulseMetricsClient()
	return d
}

// Trigger starts a StoryRun and optionally records a session keyed by req.Key.
func (d *StoryDispatcher) Trigger(ctx context.Context, req StoryTriggerRequest) (*StoryTriggerResult, error) {
	storyName := strings.TrimSpace(req.StoryName)
	if storyName == "" {
		return nil, fmt.Errorf("story name is required")
	}
	req.StoryName = storyName

	if req.Inputs == nil {
		req.Inputs = make(map[string]any)
	}
	sessionKey := strings.TrimSpace(req.Key)
	var reserved bool
	if sessionKey != "" {
		d.mu.Lock()
		if _, exists := d.sessions[sessionKey]; exists {
			d.mu.Unlock()
			return nil, ErrImpulseSessionExists
		}
		d.sessions[sessionKey] = &StorySession{Key: sessionKey}
		d.mu.Unlock()
		reserved = true
	}

	storyNamespace := strings.TrimSpace(req.StoryNamespace)
	storyRun, err := d.runtime.start(ctx, req.StoryName, storyNamespace, req.Inputs)
	d.recordTriggerStats(ctx, err)
	if err != nil {
		if reserved {
			d.Forget(sessionKey)
		}
		return nil, err
	}

	result := &StoryTriggerResult{StoryRun: storyRun}
	if sessionKey == "" {
		return result, nil
	}

	session := &StorySession{
		Key:       sessionKey,
		StoryRun:  storyRun.Name,
		Namespace: storyRun.Namespace,
		StoryName: req.StoryName,
		StartedAt: d.timeSource().UTC(),
	}
	if len(req.Metadata) > 0 {
		session.Metadata = maps.Clone(req.Metadata)
	}

	d.mu.Lock()
	d.sessions[sessionKey] = session
	d.mu.Unlock()
	result.Session = session
	return result, nil
}

// Stop cancels the StoryRun associated with the session key.
// Returns the session metadata when successful.
func (d *StoryDispatcher) Stop(ctx context.Context, key string) (*StorySession, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("session key is required")
	}

	session, ok := d.removeSession(key)
	if !ok {
		return nil, ErrImpulseSessionNotFound
	}

	err := d.runtime.stop(ctx, session.StoryRun, session.Namespace)
	switch {
	case err == nil:
		return session, nil
	case errors.Is(err, ErrStoryRunNotFound):
		return session, ErrStoryRunNotFound
	default:
		d.mu.Lock()
		d.sessions[key] = session
		d.mu.Unlock()
		return session, err
	}
}

// HasSession reports whether a session is currently tracked for the key.
func (d *StoryDispatcher) HasSession(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.sessions[key]
	return ok
}

// Session returns the session metadata for a key without mutating state.
func (d *StoryDispatcher) Session(key string) (*StorySession, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	session, ok := d.sessions[key]
	if !ok {
		return nil, false
	}
	cpy := *session
	if len(session.Metadata) > 0 {
		cpy.Metadata = maps.Clone(session.Metadata)
	}
	return &cpy, true
}

func (d *StoryDispatcher) initImpulseMetricsClient() {
	name := strings.TrimSpace(os.Getenv(contracts.ImpulseNameEnv))
	if name == "" {
		return
	}
	namespace := strings.TrimSpace(os.Getenv(contracts.ImpulseNamespaceEnv))
	client, err := k8s.NewClient()
	if err != nil {
		log.Printf("bubu sdk: unable to initialize impulse metrics client: %v", err)
		return
	}
	d.statsClient = client
	d.impulseName = name
	d.impulseNamespace = namespace
}

func (d *StoryDispatcher) recordTriggerStats(ctx context.Context, triggerErr error) {
	if d.statsClient == nil || d.impulseName == "" {
		return
	}
	delta := k8s.TriggerStatsDelta{
		TriggersReceived: 1,
		LastTrigger:      d.timeSource().UTC(),
	}
	if triggerErr != nil {
		delta.FailedTriggers = 1
	} else {
		delta.StoriesLaunched = 1
		successTime := delta.LastTrigger
		delta.LastSuccess = &successTime
	}
	if err := d.statsClient.UpdateImpulseTriggerStats(ctx, d.impulseName, d.impulseNamespace, delta); err != nil {
		log.Printf("bubu sdk: failed to update impulse trigger stats: %v", err)
	}
}

// Forget removes a session without attempting to stop the StoryRun.
func (d *StoryDispatcher) Forget(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.sessions, key)
}

func (d *StoryDispatcher) removeSession(key string) (*StorySession, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	session, ok := d.sessions[key]
	if ok {
		delete(d.sessions, key)
	}
	return session, ok
}
