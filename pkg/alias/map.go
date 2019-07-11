package alias

import (
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
)

// Value of Map
type Value struct {
	Target      string
	DisplayName string
}

// Map from real metric name to display name and target
type Map struct {
	data map[string][]Value
}

// New returns new Map
func New() *Map {
	return &Map{data: make(map[string][]Value)}
}

// Merge data from finder.Result into aliases map
func (m *Map) Merge(r finder.Result) {
	m.MergeTarget(r, "")
}

// MergeTarget data from finder.Result into aliases map
func (m *Map) MergeTarget(r finder.Result, target string) {
	series := r.Series()
	for i := 0; i < len(series); i++ {
		key := string(series[i])
		if len(key) == 0 {
			continue
		}
		abs := string(r.Abs(series[i]))
		if x, ok := m.data[key]; ok {
			m.data[key] = append(x, Value{Target: target, DisplayName: abs})
		} else {
			m.data[key] = []Value{Value{Target: target, DisplayName: abs}}
		}
	}
}

// Len returns count of keys
func (m *Map) Len() int {
	return len(m.data)
}

// Size returns count of values
func (m *Map) Size() int {
	s := 0
	for _, v := range m.data {
		s += len(v)
	}
	return s
}

// Series returns keys of aliases map
func (m *Map) Series(isReverse bool) []string {
	series := make([]string, 0, m.Len())
	for k := range m.data {
		if isReverse {
			series = append(series, reverse.String(k))
		} else {
			series = append(series, k)
		}
	}
	return series
}

// DisplayNames returns DisplayName from all Values
func (m *Map) DisplayNames() []string {
	dn := make([]string, 0, m.Size())
	for _, v := range m.data {
		for _, a := range v {
			dn = append(dn, a.DisplayName)
		}
	}
	return dn
}

// Get returns aliases for metric
func (m *Map) Get(metric string) []Value {
	return m.data[metric]
}
