// Package transport provides the different mechanisms (Kubernetes API, gRPC, etc.)
// that the SDK runtime uses to communicate with the outside world.
package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/proto"
)

// FanOutResult holds the result from a single gRPC call in a parallel fan-out scenario.
type FanOutResult struct {
	Target string
	Result *engram.Result
	Error  error
}

// StreamFanOutSession manages multiple concurrent gRPC streams, allowing a single
// Engram to broadcast messages to or receive messages from multiple downstream peers.
type StreamFanOutSession struct {
	client  *GRPCClient
	streams map[string]proto.EngramService_ExecuteStreamClient
	mutex   sync.RWMutex
}

// ExecuteParallel is a client-side utility that executes gRPC calls to multiple
// downstream Engrams in parallel and aggregates their results.
func (c *GRPCClient) ExecuteParallel(ctx context.Context, targets []GRPCTarget, execCtx *engram.ExecutionContext) (map[string]*engram.Result, error) {
	var wg sync.WaitGroup
	resultsChan := make(chan *FanOutResult, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			c.logger.Info("Fanning out to target", "endpoint", endpoint)
			result, err := c.ExecuteEngram(ctx, endpoint, execCtx)
			resultsChan <- &FanOutResult{Target: endpoint, Result: result, Error: err}
		}(target.Endpoint)
	}

	// Wait for all goroutines to finish.
	wg.Wait()
	close(resultsChan)

	finalResults := make(map[string]*engram.Result)
	var fanOutErrors []error

	// Collect results and errors from all fan-out calls.
	for res := range resultsChan {
		if res.Error != nil {
			fanOutErrors = append(fanOutErrors, res.Error)
		} else {
			finalResults[res.Target] = res.Result
		}
	}

	if len(fanOutErrors) > 0 {
		// Even if some calls succeed, we return an error if any of them failed.
		return finalResults, fmt.Errorf("one or more fan-out calls failed: %v", fanOutErrors)
	}

	return finalResults, nil
}

// CreateStreamFanOut establishes gRPC streams to multiple downstream Engrams
// and returns a session object to manage them.
func (c *GRPCClient) CreateStreamFanOut(ctx context.Context, targets []GRPCTarget) (*StreamFanOutSession, error) {
	session := &StreamFanOutSession{
		client:  c,
		streams: make(map[string]proto.EngramService_ExecuteStreamClient),
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			stream, err := c.ExecuteStream(ctx, endpoint)
			if err != nil {
				errChan <- fmt.Errorf("failed to establish stream to %s: %w", endpoint, err)
				return
			}
			session.mutex.Lock()
			session.streams[endpoint] = stream
			session.mutex.Unlock()
			c.logger.Info("Stream established", "endpoint", endpoint)
		}(target.Endpoint)
	}

	// Wait for all connection attempts to complete.
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check if any of the connection attempts failed.
	if err := <-errChan; err != nil {
		// If any stream fails to connect, we abort and close any successful connections.
		session.Close()
		return nil, err
	}

	return session, nil
}

// BroadcastMessage sends the same message to all active streams in the session concurrently.
func (s *StreamFanOutSession) BroadcastMessage(msg *engram.StreamMessage) map[string]error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var wg sync.WaitGroup
	errors := make(map[string]error, len(s.streams))
	var errMutex sync.Mutex

	s.client.logger.Info("Broadcasting message", "streamCount", len(s.streams))

	for target, stream := range s.streams {
		wg.Add(1)
		go func(t string, st proto.EngramService_ExecuteStreamClient) {
			defer wg.Done()
			if err := s.client.SendStreamMessage(st, msg); err != nil {
				errMutex.Lock()
				errors[t] = err
				errMutex.Unlock()
			}
		}(target, stream)
	}

	wg.Wait()
	if len(errors) > 0 {
		return errors
	}
	return nil
}

// SendToTarget sends a message to a single, specific target stream in the session.
func (s *StreamFanOutSession) SendToTarget(target string, msg *engram.StreamMessage) error {
	s.mutex.RLock()
	stream, ok := s.streams[target]
	s.mutex.RUnlock()

	if !ok {
		return fmt.Errorf("target %s not found in stream session", target)
	}
	return s.client.SendStreamMessage(stream, msg)
}

// ReceiveFromTarget blocks and waits to receive a message from a specific target stream.
func (s *StreamFanOutSession) ReceiveFromTarget(target string) (*engram.StreamMessage, error) {
	s.mutex.RLock()
	stream, ok := s.streams[target]
	s.mutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("target %s not found in stream session", target)
	}
	return s.client.ReceiveStreamMessage(stream)
}

// GetActiveTargets returns a slice of endpoints for all currently active streams.
func (s *StreamFanOutSession) GetActiveTargets() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	targets := make([]string, 0, len(s.streams))
	for target := range s.streams {
		targets = append(targets, target)
	}
	return targets
}

// Close gracefully closes all streams in the session by sending a CloseSend signal.
func (s *StreamFanOutSession) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.client.logger.Info("Closing streams", "streamCount", len(s.streams))
	for _, stream := range s.streams {
		stream.CloseSend()
	}
	// Clear the streams map.
	s.streams = make(map[string]proto.EngramService_ExecuteStreamClient)
}
