package sdk

import (
	"fmt"
	"strings"
	"sync"
	"time"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/proto"
)

const (
	defaultChunkReassemblyTTL     = 2 * time.Minute
	defaultChunkMaxInFlightChunks = 256
	defaultChunkMaxAssemblyChunks = 256
	defaultChunkMaxInFlightBytes  = 64 * 1024 * 1024
	defaultChunkMaxChunkBytes     = DefaultMaxMessageSize
)

type publishChunkReassembler struct {
	mu            sync.Mutex
	ttl           time.Duration
	sweepInterval time.Duration
	maxChunks     int
	maxBytes      int
	maxChunkBytes int
	maxAssembly   int
	inflightBytes int
	assemblies    map[string]*publishChunkAssembly
	stopCh        chan struct{}
	doneCh        chan struct{}
}

type publishChunkAssembly struct {
	lastSeen     time.Time
	expected     uint32
	received     uint32
	totalBytes   uint32
	base         *transportpb.PublishRequest
	chunks       [][]byte
	receivedByte int
}

func newPublishChunkReassembler(ttl time.Duration, maxChunks int, maxBytes int) *publishChunkReassembler {
	if ttl <= 0 {
		ttl = defaultChunkReassemblyTTL
	}
	if maxChunks <= 0 {
		maxChunks = defaultChunkMaxInFlightChunks
	}
	if maxBytes <= 0 {
		maxBytes = defaultChunkMaxInFlightBytes
	}
	reassembler := &publishChunkReassembler{
		ttl:           ttl,
		sweepInterval: chunkReassemblySweepInterval(ttl),
		maxChunks:     maxChunks,
		maxBytes:      maxBytes,
		maxChunkBytes: defaultChunkMaxChunkBytes,
		maxAssembly:   defaultChunkMaxAssemblyChunks,
		assemblies:    make(map[string]*publishChunkAssembly),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	go reassembler.runEvictionLoop()
	return reassembler
}

//nolint:gocyclo,lll
func (r *publishChunkReassembler) Add(req *transportpb.PublishRequest) (*transportpb.PublishRequest, bool, error) {
	if r == nil || req == nil {
		return nil, false, nil
	}
	env := req.GetEnvelope()
	if !isChunkedEnvelope(env) {
		return req, true, nil
	}
	if env == nil {
		return nil, false, fmt.Errorf("chunked publish request missing envelope")
	}
	chunkID := strings.TrimSpace(env.GetChunkId())
	if chunkID == "" {
		return nil, false, fmt.Errorf("chunked publish request missing chunk_id")
	}
	streamID := strings.TrimSpace(env.GetStreamId())
	if streamID == "" {
		return nil, false, fmt.Errorf("chunked publish request missing stream_id")
	}
	count := env.GetChunkCount()
	index := env.GetChunkIndex()
	if count == 0 {
		return nil, false, fmt.Errorf("chunked publish request missing chunk_count")
	}
	if index >= count {
		return nil, false, fmt.Errorf("chunk_index %d out of range (count %d)", index, count)
	}
	if maxAssembly := r.maxAssembly; maxAssembly > 0 && count > uint32(maxAssembly) {
		return nil, false, fmt.Errorf("chunk_count %d exceeds max chunks per assembly %d", count, maxAssembly)
	}

	binary := req.GetBinary()
	if binary == nil {
		return nil, false, fmt.Errorf("chunked publish request missing binary frame")
	}
	payload := binary.GetPayload()
	if maxChunk := r.maxChunkBytes; maxChunk > 0 && len(payload) > maxChunk {
		return nil, false, fmt.Errorf("chunk payload %d exceeds max chunk size %d", len(payload), maxChunk)
	}
	if env.GetChunkBytes() > 0 && int(env.GetChunkBytes()) != len(payload) {
		return nil, false, fmt.Errorf("chunk_bytes %d does not match payload size %d", env.GetChunkBytes(), len(payload))
	}

	key := chunkKey(streamID, env.GetPartition(), chunkID)
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictExpiredLocked(now)
	assembly := r.assemblies[key]
	if assembly == nil {
		if len(r.assemblies) >= r.maxChunks {
			return nil, false, fmt.Errorf("too many chunk assemblies in flight")
		}
		if r.inflightBytes+len(payload) > r.maxBytes {
			return nil, false, fmt.Errorf("chunk reassembly buffer exceeded (%d > %d)", r.inflightBytes+len(payload), r.maxBytes)
		}
		assembly = &publishChunkAssembly{
			lastSeen:   now,
			expected:   count,
			totalBytes: env.GetTotalBytes(),
			base:       proto.Clone(req).(*transportpb.PublishRequest),
			chunks:     make([][]byte, count),
		}
		r.assemblies[key] = assembly
	}

	if assembly.expected != count {
		return nil, false, fmt.Errorf("chunk_count mismatch for %s: %d != %d", chunkID, count, assembly.expected)
	}
	if assembly.chunks[index] != nil {
		// Duplicate chunk; ignore.
		assembly.lastSeen = now
		return nil, false, nil
	}
	if r.inflightBytes+len(payload) > r.maxBytes {
		return nil, false, fmt.Errorf("chunk reassembly buffer exceeded (%d > %d)", r.inflightBytes+len(payload), r.maxBytes)
	}
	assembly.chunks[index] = append([]byte(nil), payload...)
	assembly.received++
	assembly.receivedByte += len(payload)
	assembly.lastSeen = now
	r.inflightBytes += len(payload)

	if assembly.received < assembly.expected {
		return nil, false, nil
	}

	assembled, err := reassemblePublishRequest(assembly)
	if err != nil {
		delete(r.assemblies, key)
		r.inflightBytes -= assembly.receivedByte
		return nil, false, err
	}
	delete(r.assemblies, key)
	r.inflightBytes -= assembly.receivedByte
	return assembled, true, nil
}

func chunkReassemblySweepInterval(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	interval := ttl / 2
	if interval <= 0 {
		return ttl
	}
	return interval
}

func (r *publishChunkReassembler) runEvictionLoop() {
	if r == nil {
		return
	}
	defer close(r.doneCh)
	if r.ttl <= 0 || r.sweepInterval <= 0 {
		return
	}
	ticker := time.NewTicker(r.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.evictExpired(time.Now())
		case <-r.stopCh:
			return
		}
	}
}

func (r *publishChunkReassembler) Stop() {
	if r == nil {
		return
	}
	select {
	case <-r.doneCh:
		return
	default:
	}
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
	<-r.doneCh
}

func reassemblePublishRequest(assembly *publishChunkAssembly) (*transportpb.PublishRequest, error) {
	if assembly == nil || assembly.base == nil {
		return nil, fmt.Errorf("chunk assembly missing base request")
	}
	payloadLen := 0
	for i := uint32(0); i < assembly.expected; i++ {
		chunk := assembly.chunks[i]
		if chunk == nil {
			return nil, fmt.Errorf("chunk %d missing", i)
		}
		payloadLen += len(chunk)
	}
	if assembly.totalBytes > 0 && payloadLen != int(assembly.totalBytes) {
		return nil, fmt.Errorf("total_bytes %d does not match assembled payload %d", assembly.totalBytes, payloadLen)
	}
	payload := make([]byte, 0, payloadLen)
	for _, chunk := range assembly.chunks {
		payload = append(payload, chunk...)
	}
	cloned := proto.Clone(assembly.base).(*transportpb.PublishRequest)
	binary := cloned.GetBinary()
	if binary == nil {
		return nil, fmt.Errorf("chunked publish request missing binary frame")
	}
	binary.Payload = payload
	clearChunkFields(cloned.GetEnvelope())
	return cloned, nil
}

func clearChunkFields(env *transportpb.StreamEnvelope) {
	if env == nil {
		return
	}
	env.ChunkId = ""
	env.ChunkIndex = 0
	env.ChunkCount = 0
	env.ChunkBytes = 0
	env.TotalBytes = 0
}

func isChunkedEnvelope(env *transportpb.StreamEnvelope) bool {
	if env == nil {
		return false
	}
	return env.GetChunkId() != "" || env.GetChunkCount() > 0 || env.GetChunkIndex() > 0 || env.GetChunkBytes() > 0 || env.GetTotalBytes() > 0 //nolint:lll
}

func chunkKey(streamID, partition, chunkID string) string {
	if partition == "" {
		return streamID + "|" + chunkID
	}
	return streamID + "|" + partition + "|" + chunkID
}

func (r *publishChunkReassembler) evictExpired(now time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictExpiredLocked(now)
}

func (r *publishChunkReassembler) evictExpiredLocked(now time.Time) {
	if r == nil || r.ttl <= 0 {
		return
	}
	for key, assembly := range r.assemblies {
		if assembly == nil {
			delete(r.assemblies, key)
			continue
		}
		if now.Sub(assembly.lastSeen) > r.ttl {
			delete(r.assemblies, key)
			r.inflightBytes -= assembly.receivedByte
		}
	}
}
