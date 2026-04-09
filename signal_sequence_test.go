package sdk

import "testing"

func TestSignalSequenceNext(t *testing.T) {
	seq := NewSignalSequence(0)
	if got := seq.Next(); got != 1 {
		t.Fatalf("Next() = %d", got)
	}
	if got := seq.Next(); got != 2 {
		t.Fatalf("Next() = %d", got)
	}
}

func TestSignalSequenceStart(t *testing.T) {
	seq := NewSignalSequence(10)
	if got := seq.Next(); got != 11 {
		t.Fatalf("Next() = %d", got)
	}
}
