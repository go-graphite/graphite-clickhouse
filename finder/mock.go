package finder

import (
	"bytes"
	"context"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
)

// MockFinder is used for testing purposes
type MockFinder struct {
	fnd   Finder
	query string // logged from execute
}

// NewMockFinder returns new MockFinder object with given result
func NewMockFinder(result [][]byte) *MockFinder {
	return &MockFinder{
		fnd: NewCachedIndex(bytes.Join(result, []byte{'\n'})),
	}
}

// NewMockTagged returns new MockFinder object with given result
func NewMockTagged(result [][]byte) *MockFinder {
	return &MockFinder{
		fnd: NewCachedTags(bytes.Join(result, []byte{'\n'})),
	}
}

// Execute assigns given query to the query field
func (m *MockFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	m.query = query
	return
}

// List returns the result
func (m *MockFinder) List() [][]byte {
	return m.fnd.List()
}

// Series returns the result
func (m *MockFinder) Series() [][]byte {
	return m.fnd.Series()
}

// Abs returns the same given v
func (m *MockFinder) Abs(v []byte) []byte {
	return m.fnd.Abs(v)
}

func (m *MockFinder) Bytes() ([]byte, error) {
	return m.fnd.Bytes()
}

// Strings returns the result converted to []string
func (m *MockFinder) Strings() []string {
	body, _ := m.fnd.Bytes()
	return strings.Split(string(body), "\n")
}
