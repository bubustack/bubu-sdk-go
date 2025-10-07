package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/stretchr/testify/mock"
)

// mockStore implements the Store interface for testing
type mockStore struct {
	data   map[string][]byte
	writes int
	reads  int
}

// MockManager is a mock implementation of the StorageManager interface.
type MockManager struct {
	mock.Mock
}

// Hydrate mocks the Hydrate method for testing.
func (m *MockManager) Hydrate(ctx context.Context, data any) (any, error) {
	args := m.Called(ctx, data)
	return args.Get(0), args.Error(1)
}

// Dehydrate mocks the Dehydrate method for testing.
func (m *MockManager) Dehydrate(ctx context.Context, data any, stepRunID string) (any, error) {
	args := m.Called(ctx, data, stepRunID)
	return args.Get(0), args.Error(1)
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string][]byte),
	}
}

// Write mocks the Write method for testing.
func (m *mockStore) Write(ctx context.Context, path string, reader io.Reader) error {
	m.writes++
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.data[path] = data
	return nil
}

// Read mocks the Read method for testing.
func (m *mockStore) Read(ctx context.Context, path string, writer io.Writer) error {
	m.reads++
	data, exists := m.data[path]
	if !exists {
		return fmt.Errorf("path not found: %s", path)
	}
	_, err := writer.Write(data)
	return err
}
