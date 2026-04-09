package conformance

import (
	"context"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	sdk "github.com/bubustack/bubu-sdk-go"
	"github.com/bubustack/bubu-sdk-go/engram"
)

type errorStreamEngram struct{}

func (e *errorStreamEngram) Init(ctx context.Context, cfg streamConfig, secrets *engram.Secrets) error {
	return nil
}

func (e *errorStreamEngram) Stream(ctx context.Context, in <-chan engram.InboundMessage, out chan<- engram.StreamMessage) error { //nolint:lll
	_ = in
	msg, err := sdk.NewStreamErrorMessage(runsv1alpha1.StructuredError{
		Version: runsv1alpha1.StructuredErrorVersionV1,
		Type:    runsv1alpha1.StructuredErrorTypeExecution,
		Message: "stream error",
	})
	if err != nil {
		return err
	}
	out <- msg
	return nil
}

func TestStreamSuiteErrorEnvelope(t *testing.T) {
	suite := StreamSuite[streamConfig]{
		Engram:               &errorStreamEngram{},
		Inputs:               []engram.StreamMessage{{Payload: []byte(`{"ok":true}`)}},
		RequireValidMessages: true,
	}
	suite.Run(t)
}

type invalidOutputOnErrorStreamEngram struct{}

func (e *invalidOutputOnErrorStreamEngram) Init(ctx context.Context, cfg streamConfig, secrets *engram.Secrets) error {
	return nil
}

func (e *invalidOutputOnErrorStreamEngram) Stream(
	ctx context.Context,
	in <-chan engram.InboundMessage,
	out chan<- engram.StreamMessage,
) error {
	_ = ctx
	_ = in
	out <- engram.StreamMessage{}
	return sdk.NewStructuredError(
		runsv1alpha1.StructuredErrorTypeExecution,
		"stream failed",
	)
}

func TestStreamSuiteExpectedErrorStillValidatesOutputs(t *testing.T) {
	suite := StreamSuite[streamConfig]{
		Engram:               &invalidOutputOnErrorStreamEngram{},
		Inputs:               []engram.StreamMessage{{Payload: []byte(`{"ok":true}`)}},
		RequireValidMessages: true,
		ExpectError:          true,
	}

	err := suite.validateOutcome([]engram.StreamMessage{{}}, sdk.NewStructuredError(
		runsv1alpha1.StructuredErrorTypeExecution,
		"stream failed",
	), 0)
	if err == nil {
		t.Fatal("expected suite validation to fail for invalid output on error path")
	}
}
