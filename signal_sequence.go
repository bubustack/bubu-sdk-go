package sdk

import (
	"context"
	"sync/atomic"
	"time"
)

// SequencedSignal wraps a signal payload with a monotonically increasing sequence.
type SequencedSignal struct {
	// Seq is the monotonic sequence number assigned to the signal event.
	Seq uint64 `json:"seq"`
	// EmittedAt records when the signal was emitted in UTC.
	EmittedAt time.Time `json:"emittedAt,omitempty"`
	// Value carries the original signal payload associated with Seq.
	Value any `json:"value,omitempty"`
}

// SignalSequence is a process-local monotonic sequence generator for signals.
type SignalSequence struct {
	counter atomic.Uint64
}

// NewSignalSequence constructs a SignalSequence starting at the provided value.
func NewSignalSequence(start uint64) *SignalSequence {
	seq := &SignalSequence{}
	if start > 0 {
		seq.counter.Store(start)
	}
	return seq
}

// Next increments and returns the next sequence value.
func (s *SignalSequence) Next() uint64 {
	if s == nil {
		return 0
	}
	return s.counter.Add(1)
}

// EmitSequencedSignal emits a signal wrapped with a sequence number and timestamp.
func EmitSequencedSignal(ctx context.Context, key string, seq uint64, value any) error {
	payload := SequencedSignal{
		Seq:       seq,
		EmittedAt: time.Now().UTC(),
		Value:     value,
	}
	return emitSignalEvent(ctx, key, payload, seq)
}

// EmitSignalWithSequence emits a signal using the provided sequence generator.
func EmitSignalWithSequence(ctx context.Context, key string, seq *SignalSequence, value any) error {
	if seq == nil {
		return EmitSignal(ctx, key, value)
	}
	return EmitSequencedSignal(ctx, key, seq.Next(), value)
}
