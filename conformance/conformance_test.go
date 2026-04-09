package conformance

import (
	"context"
	"errors"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	sdk "github.com/bubustack/bubu-sdk-go"
	"github.com/bubustack/bubu-sdk-go/engram"
)

type contextKey string

type testBatchEngram struct{}

type testConfig struct{}

type testInputs struct{}

func (t *testBatchEngram) Init(ctx context.Context, cfg testConfig, secrets *engram.Secrets) error {
	return nil
}

func (t *testBatchEngram) Process(ctx context.Context, execCtx *engram.ExecutionContext, inputs testInputs) (*engram.Result, error) { //nolint:lll
	return engram.NewResultFrom(map[string]any{"ok": true}), nil
}

type ctxCheckingBatchEngram struct {
	key      contextKey
	gotValue any
}

func (e *ctxCheckingBatchEngram) Init(ctx context.Context, cfg testConfig, secrets *engram.Secrets) error {
	e.gotValue = ctx.Value(e.key)
	return nil
}

func (e *ctxCheckingBatchEngram) Process(ctx context.Context, execCtx *engram.ExecutionContext, inputs testInputs) (*engram.Result, error) { //nolint:lll
	e.gotValue = ctx.Value(e.key)
	return engram.NewResultFrom(map[string]any{"ok": true}), nil
}

func TestBatchSuiteRun(t *testing.T) {
	suite := BatchSuite[testConfig, testInputs]{
		Engram: &testBatchEngram{},
	}
	suite.Run(t)
}

func TestBatchSuiteRun_UsesProvidedContext(t *testing.T) {
	key := contextKey("tenant")
	ctx := context.WithValue(context.Background(), key, "content-digest")
	eng := &ctxCheckingBatchEngram{key: key}

	BatchSuite[testConfig, testInputs]{
		Engram:  eng,
		Context: ctx,
	}.Run(t)

	if eng.gotValue != "content-digest" {
		t.Fatalf("expected provided context value to propagate, got %v", eng.gotValue)
	}
}

type errorBatchEngram struct{}

func (t *errorBatchEngram) Init(ctx context.Context, cfg testConfig, secrets *engram.Secrets) error {
	return nil
}

func (t *errorBatchEngram) Process(ctx context.Context, execCtx *engram.ExecutionContext, inputs testInputs) (*engram.Result, error) { //nolint:lll
	return nil, errors.New("boom")
}

type structuredErrorBatchEngram struct{}

func (t *structuredErrorBatchEngram) Init(ctx context.Context, cfg testConfig, secrets *engram.Secrets) error {
	return nil
}

func (t *structuredErrorBatchEngram) Process(ctx context.Context, execCtx *engram.ExecutionContext, inputs testInputs) (*engram.Result, error) { //nolint:lll
	return nil, sdk.NewStructuredError(runsv1alpha1.StructuredErrorTypeExecution, "boom")
}

func TestBatchSuiteRun_FailsOnUnexpectedError(t *testing.T) {
	err := BatchSuite[testConfig, testInputs]{
		Engram: &errorBatchEngram{},
	}.validateOutcome(nil, errors.New("boom"))
	if err == nil {
		t.Fatal("expected BatchSuite to fail on unexpected errors")
	}
}

func TestBatchSuiteRun_FailsWhenStructuredErrorRequiredButRunSucceeds(t *testing.T) {
	err := BatchSuite[testConfig, testInputs]{
		Engram:                 &testBatchEngram{},
		RequireStructuredError: true,
	}.validateOutcome(engram.NewResultFrom(map[string]any{"ok": true}), nil)
	if err == nil {
		t.Fatal("expected BatchSuite to fail when a structured error was required but execution succeeded")
	}
}

func TestBatchSuiteRun_AllowsStructuredErrorWhenRequired(t *testing.T) {
	err := BatchSuite[testConfig, testInputs]{
		Engram:                 &structuredErrorBatchEngram{},
		RequireStructuredError: true,
	}.validateOutcome(nil, sdk.NewStructuredError(runsv1alpha1.StructuredErrorTypeExecution, "boom"))
	if err != nil {
		t.Fatalf("expected BatchSuite to accept structured errors when required, got: %v", err)
	}
}

func TestBatchSuiteRun_AllowsExpectedPlainError(t *testing.T) {
	err := BatchSuite[testConfig, testInputs]{
		Engram:      &errorBatchEngram{},
		ExpectError: true,
	}.validateOutcome(nil, errors.New("boom"))
	if err != nil {
		t.Fatalf("expected BatchSuite to accept expected plain errors, got: %v", err)
	}
}

func TestBatchSuiteRun_ValidatesExpectedError(t *testing.T) {
	var validated bool
	err := BatchSuite[testConfig, testInputs]{
		Engram:      &errorBatchEngram{},
		ExpectError: true,
		ValidateError: func(err error) error {
			validated = true
			if err == nil || err.Error() != "boom" {
				t.Fatalf("unexpected error passed to ValidateError: %v", err)
			}
			return nil
		},
	}.validateOutcome(nil, errors.New("boom"))
	if err != nil {
		t.Fatalf("expected BatchSuite to accept validated error, got: %v", err)
	}
	if !validated {
		t.Fatal("expected ValidateError to run")
	}
}

type testStreamEngram struct{}

type streamConfig struct{}

func (t *testStreamEngram) Init(ctx context.Context, cfg streamConfig, secrets *engram.Secrets) error {
	return nil
}

func (t *testStreamEngram) Stream(ctx context.Context, in <-chan engram.InboundMessage, out chan<- engram.StreamMessage) error { //nolint:lll
	for msg := range in {
		out <- msg.StreamMessage
		msg.Done()
	}
	return nil
}

type ctxCheckingStreamEngram struct {
	key      contextKey
	gotValue any
}

func (e *ctxCheckingStreamEngram) Init(ctx context.Context, cfg streamConfig, secrets *engram.Secrets) error {
	e.gotValue = ctx.Value(e.key)
	return nil
}

func (e *ctxCheckingStreamEngram) Stream(ctx context.Context, in <-chan engram.InboundMessage, out chan<- engram.StreamMessage) error { //nolint:lll
	e.gotValue = ctx.Value(e.key)
	for msg := range in {
		msg.Done()
	}
	return nil
}

func TestStreamSuiteRun(t *testing.T) {
	suite := StreamSuite[streamConfig]{
		Engram:               &testStreamEngram{},
		Inputs:               []engram.StreamMessage{{Payload: []byte(`{"ok":true}`)}},
		RequireValidMessages: true,
	}
	suite.Run(t)
}

func TestStreamSuiteRun_UsesProvidedContext(t *testing.T) {
	key := contextKey("tenant")
	ctx := context.WithValue(context.Background(), key, "content-digest")
	eng := &ctxCheckingStreamEngram{key: key}

	StreamSuite[streamConfig]{
		Engram:               eng,
		Context:              ctx,
		Inputs:               []engram.StreamMessage{{Payload: []byte(`{"ok":true}`)}},
		RequireValidMessages: true,
	}.Run(t)

	if eng.gotValue != "content-digest" {
		t.Fatalf("expected provided context value to propagate, got %v", eng.gotValue)
	}
}

func TestStreamSuiteValidateOutputContract_DefaultAllowsEmpty(t *testing.T) {
	err := (StreamSuite[streamConfig]{}).validateOutputContract(nil)
	if err != nil {
		t.Fatalf("expected default stream output contract to allow empty output, got: %v", err)
	}
}

func TestStreamSuiteValidateOutputContract_RequireNonEmptyOutput(t *testing.T) {
	err := (StreamSuite[streamConfig]{RequireNonEmptyOutput: true}).validateOutputContract(nil)
	if err == nil {
		t.Fatal("expected stream output contract to reject empty output when RequireNonEmptyOutput=true")
	}
}

func TestStreamSuiteValidateOutputContract_MinOutputCount(t *testing.T) {
	outputs := []engram.StreamMessage{{Payload: []byte(`{"ok":true}`)}}
	err := (StreamSuite[streamConfig]{MinOutputCount: 2}).validateOutputContract(outputs)
	if err == nil {
		t.Fatal("expected stream output contract to reject outputs below MinOutputCount")
	}
}

func TestStreamSuiteValidateOutputContract_MinOutputCountSatisfied(t *testing.T) {
	outputs := []engram.StreamMessage{
		{Payload: []byte(`{"ok":true}`)},
		{Payload: []byte(`{"ok":true}`)},
	}
	err := (StreamSuite[streamConfig]{RequireNonEmptyOutput: true, MinOutputCount: 2}).validateOutputContract(outputs)
	if err != nil {
		t.Fatalf("expected stream output contract to pass when requirements are met, got: %v", err)
	}
}

func TestStreamSuiteRun_RequiresAllInputsDone(t *testing.T) {
	err := (StreamSuite[streamConfig]{
		Inputs:               []engram.StreamMessage{{Payload: []byte(`{"ok":true}`)}},
		RequireAllInputsDone: true,
	}).validateInputAcknowledgements(0)
	if err == nil {
		t.Fatal("expected input acknowledgement validation to fail when inputs are not acknowledged")
	}
}

func TestStreamSuiteValidateOutcome_FailsOnUnexpectedError(t *testing.T) {
	err := (StreamSuite[streamConfig]{}).validateOutcome(nil, errors.New("boom"), 0)
	if err == nil {
		t.Fatal("expected StreamSuite to fail on unexpected errors")
	}
}

func TestStreamSuiteValidateOutcome_AllowsExpectedPlainError(t *testing.T) {
	err := (StreamSuite[streamConfig]{ExpectError: true}).validateOutcome(nil, errors.New("boom"), 0)
	if err != nil {
		t.Fatalf("expected StreamSuite to accept expected plain errors, got: %v", err)
	}
}

func TestStreamSuiteValidateOutcome_AllowsStructuredErrorWhenRequired(t *testing.T) {
	err := (StreamSuite[streamConfig]{RequireStructuredError: true}).validateOutcome(
		nil,
		sdk.NewStructuredError(runsv1alpha1.StructuredErrorTypeExecution, "boom"),
		0,
	)
	if err != nil {
		t.Fatalf("expected StreamSuite to accept structured errors when required, got: %v", err)
	}
}

func TestStreamSuiteValidateOutcome_ValidatesExpectedError(t *testing.T) {
	var validated bool
	err := (StreamSuite[streamConfig]{
		ExpectError: true,
		ValidateError: func(err error) error {
			validated = true
			if err == nil || err.Error() != "boom" {
				t.Fatalf("unexpected error passed to ValidateError: %v", err)
			}
			return nil
		},
	}).validateOutcome(nil, errors.New("boom"), 0)
	if err != nil {
		t.Fatalf("expected StreamSuite to accept validated error, got: %v", err)
	}
	if !validated {
		t.Fatal("expected ValidateError to run")
	}
}
