package finder

import (
	"context"
)

// MockFinder is used for testing purposes
type MockFinder struct {
	result [][]byte // from new
	query  string   // logged from execute
}

// NewMockFinder returns new MockFinder object with given result
func NewMockFinder(result [][]byte) *MockFinder {
	return &MockFinder{
		result: result,
	}
}

// Execute assigns given query to the query field
func (m *MockFinder) Execute(ctx context.Context, query string, from int64, until int64) error {
	m.query = query
	return nil
}

// List returns the result
func (m *MockFinder) List() [][]byte {
	return m.result
}

// Series returns the result
func (m *MockFinder) Series() [][]byte {
	return m.result
}

// Abs returns the same given v
func (m *MockFinder) Abs(v []byte) []byte {
	return v
}

// Strings returns the result converted to []string
func (m *MockFinder) Strings() (result []string) {
	result = make([]string, len(m.result))
	for i := range m.result {
		result[i] = string(m.result[i])
	}
	return
}
