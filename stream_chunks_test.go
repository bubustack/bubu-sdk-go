package sdk

import (
	"fmt"
	"testing"
	"time"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
)

func TestPublishChunkReassembler(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 0)
	t.Cleanup(reassembler.Stop)

	baseEnv := &transportpb.StreamEnvelope{
		StreamId:   "stream-1",
		Sequence:   42,
		Partition:  "p0",
		ChunkId:    "chunk-1",
		ChunkCount: 2,
	}

	req1 := &transportpb.PublishRequest{
		Metadata: map[string]string{"k": "v"},
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload:  []byte("hello"),
			MimeType: "application/octet-stream",
		}},
	}
	req1.Envelope.ChunkIndex = 0
	req1.Envelope.ChunkBytes = uint32(len(req1.GetBinary().GetPayload()))

	result, complete, err := reassembler.Add(req1)
	require.NoError(t, err)
	require.False(t, complete)
	require.Nil(t, result)

	req2 := &transportpb.PublishRequest{
		Metadata: map[string]string{"k": "v"},
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload:  []byte("world"),
			MimeType: "application/octet-stream",
		}},
	}
	req2.Envelope.ChunkIndex = 1
	req2.Envelope.ChunkBytes = uint32(len(req2.GetBinary().GetPayload()))

	result, complete, err = reassembler.Add(req2)
	require.NoError(t, err)
	require.True(t, complete)
	require.NotNil(t, result)

	binary := result.GetBinary()
	require.NotNil(t, binary)
	require.Equal(t, []byte("helloworld"), binary.GetPayload())
	require.Equal(t, "application/octet-stream", binary.GetMimeType())

	env := result.GetEnvelope()
	require.NotNil(t, env)
	require.Equal(t, "stream-1", env.GetStreamId())
	require.Equal(t, uint64(42), env.GetSequence())
	require.Equal(t, "p0", env.GetPartition())
	require.Empty(t, env.GetChunkId())
	require.Equal(t, uint32(0), env.GetChunkCount())
	require.Equal(t, uint32(0), env.GetChunkIndex())
}

func TestPublishChunkReassemblerRejectsExcessiveChunkCount(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 0)
	t.Cleanup(reassembler.Stop)
	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:   "stream-1",
			ChunkId:    "chunk-1",
			ChunkCount: uint32(defaultChunkMaxAssemblyChunks + 1),
			ChunkIndex: 0,
			ChunkBytes: 1,
		},
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload: []byte("a"),
		}},
	}

	_, _, err := reassembler.Add(req)
	require.ErrorContains(t, err, "exceeds max chunks per assembly")
}

func TestPublishChunkReassemblerEnforcesByteLimitAcrossChunks(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 6)
	t.Cleanup(reassembler.Stop)

	baseEnv := &transportpb.StreamEnvelope{
		StreamId:   "stream-1",
		ChunkId:    "chunk-1",
		ChunkCount: 2,
	}

	req1 := &transportpb.PublishRequest{
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload: []byte("four"),
		}},
	}
	req1.Envelope.ChunkIndex = 0
	req1.Envelope.ChunkBytes = uint32(len(req1.GetBinary().GetPayload()))

	_, complete, err := reassembler.Add(req1)
	require.NoError(t, err)
	require.False(t, complete)

	req2 := &transportpb.PublishRequest{
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload: []byte("four"),
		}},
	}
	req2.Envelope.ChunkIndex = 1
	req2.Envelope.ChunkBytes = uint32(len(req2.GetBinary().GetPayload()))

	_, _, err = reassembler.Add(req2)
	require.ErrorContains(t, err, "chunk reassembly buffer exceeded")
}

func TestPublishChunkReassemblerDuplicateChunkIsIdempotent(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 0)
	t.Cleanup(reassembler.Stop)

	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:   "stream-1",
			ChunkId:    "chunk-dup",
			ChunkCount: 2,
			ChunkIndex: 0,
			ChunkBytes: 5,
		},
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload: []byte("hello"),
		}},
	}

	_, complete, err := reassembler.Add(req)
	require.NoError(t, err)
	require.False(t, complete)

	// Send same chunk again — should be no-op
	_, complete, err = reassembler.Add(req)
	require.NoError(t, err)
	require.False(t, complete)

	// Inflight bytes should count only once
	require.Equal(t, 5, reassembler.inflightBytes)
}

func TestPublishChunkReassemblerMaxAssembliesInFlight(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 0)
	t.Cleanup(reassembler.Stop)

	// Override maxChunks to a small value for testing
	reassembler.maxChunks = 2

	for i := range uint32(2) {
		req := &transportpb.PublishRequest{
			Envelope: &transportpb.StreamEnvelope{
				StreamId:   fmt.Sprintf("stream-%d", i),
				ChunkId:    fmt.Sprintf("chunk-%d", i),
				ChunkCount: 2,
				ChunkIndex: 0,
				ChunkBytes: 1,
			},
			Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
				Payload: []byte("x"),
			}},
		}
		_, _, err := reassembler.Add(req)
		require.NoError(t, err)
	}

	// Third assembly should be rejected
	overflow := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:   "stream-overflow",
			ChunkId:    "chunk-overflow",
			ChunkCount: 2,
			ChunkIndex: 0,
			ChunkBytes: 1,
		},
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload: []byte("x"),
		}},
	}
	_, _, err := reassembler.Add(overflow)
	require.ErrorContains(t, err, "too many chunk assemblies in flight")
}

func TestPublishChunkReassemblerMaxChunkBytes(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 0)
	t.Cleanup(reassembler.Stop)
	reassembler.maxChunkBytes = 3

	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:   "stream-1",
			ChunkId:    "chunk-big",
			ChunkCount: 2,
			ChunkIndex: 0,
			ChunkBytes: 10,
		},
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload: []byte("toolarge!!"),
		}},
	}

	_, _, err := reassembler.Add(req)
	require.ErrorContains(t, err, "exceeds max chunk size")
}

func TestPublishChunkReassemblerValidationErrors(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 0)
	t.Cleanup(reassembler.Stop)

	tests := []struct {
		name    string
		req     *transportpb.PublishRequest
		wantErr string
	}{
		{
			name:    "nil request",
			req:     nil,
			wantErr: "",
		},
		{
			name: "zero chunk_count",
			req: &transportpb.PublishRequest{
				Envelope: &transportpb.StreamEnvelope{
					StreamId:   "s",
					ChunkId:    "c",
					ChunkCount: 0,
				},
				Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{Payload: []byte("a")}},
			},
			wantErr: "missing chunk_count",
		},
		{
			name: "chunk_index out of range",
			req: &transportpb.PublishRequest{
				Envelope: &transportpb.StreamEnvelope{
					StreamId:   "s",
					ChunkId:    "c",
					ChunkCount: 2,
					ChunkIndex: 5,
				},
				Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{Payload: []byte("a")}},
			},
			wantErr: "out of range",
		},
		{
			name: "missing binary frame",
			req: &transportpb.PublishRequest{
				Envelope: &transportpb.StreamEnvelope{
					StreamId:   "s",
					ChunkId:    "c",
					ChunkCount: 2,
					ChunkIndex: 0,
				},
			},
			wantErr: "missing binary frame",
		},
		{
			name: "chunk_bytes mismatch",
			req: &transportpb.PublishRequest{
				Envelope: &transportpb.StreamEnvelope{
					StreamId:   "s",
					ChunkId:    "c",
					ChunkCount: 2,
					ChunkIndex: 0,
					ChunkBytes: 999,
				},
				Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{Payload: []byte("a")}},
			},
			wantErr: "does not match payload size",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, complete, err := reassembler.Add(tt.req)
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Nil(t, result)
				require.False(t, complete)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestPublishChunkReassemblerChunkCountMismatch(t *testing.T) {
	reassembler := newPublishChunkReassembler(time.Minute, 0, 0)
	t.Cleanup(reassembler.Stop)

	req1 := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:   "s",
			ChunkId:    "c",
			ChunkCount: 2,
			ChunkIndex: 0,
			ChunkBytes: 1,
		},
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{Payload: []byte("a")}},
	}
	_, _, err := reassembler.Add(req1)
	require.NoError(t, err)

	req2 := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:   "s",
			ChunkId:    "c",
			ChunkCount: 3, // different count
			ChunkIndex: 1,
			ChunkBytes: 1,
		},
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{Payload: []byte("b")}},
	}
	_, _, err = reassembler.Add(req2)
	require.ErrorContains(t, err, "chunk_count mismatch")
}

func TestPublishChunkReassemblerEvictsStaleAssembliesWithoutNewChunks(t *testing.T) {
	reassembler := newPublishChunkReassembler(20*time.Millisecond, 0, 0)
	t.Cleanup(reassembler.Stop)

	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:   "stream-1",
			ChunkId:    "stale-chunk",
			ChunkCount: 2,
			ChunkIndex: 0,
			ChunkBytes: 4,
		},
		Frame: &transportpb.PublishRequest_Binary{Binary: &transportpb.BinaryFrame{
			Payload: []byte("wait"),
		}},
	}

	result, complete, err := reassembler.Add(req)
	require.NoError(t, err)
	require.False(t, complete)
	require.Nil(t, result)
	require.Len(t, reassembler.assemblies, 1)
	require.Equal(t, len(req.GetBinary().GetPayload()), reassembler.inflightBytes)

	require.Eventually(t, func() bool {
		reassembler.mu.Lock()
		defer reassembler.mu.Unlock()
		return len(reassembler.assemblies) == 0 && reassembler.inflightBytes == 0
	}, 250*time.Millisecond, 10*time.Millisecond)
}
