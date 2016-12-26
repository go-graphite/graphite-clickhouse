package finder

type MockFinder struct {
	result [][]byte // from new
	query  string   // logged from execute
}

func NewMockFinder(result [][]byte) *MockFinder {
	return &MockFinder{
		result: result,
	}
}

func (m *MockFinder) Execute(query string) error {
	m.query = query
	return nil
}

func (m *MockFinder) List() [][]byte {
	return m.result
}

func (m *MockFinder) Series() [][]byte {
	return m.result
}

func (m *MockFinder) Abs(v []byte) ([]byte, bool) {
	return v, false
}
