package alias

import (
	"bytes"
	"strings"
	"sync"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/utils"
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
	lock sync.RWMutex
}

// New returns new Map
func New() *Map {
	return &Map{
		data: make(map[string][]Value),
		lock: sync.RWMutex{},
	}
}

// Merge data from finder.Result into aliases map
func (m *Map) Merge(r finder.Result, useCache bool) {
	m.MergeTarget(r, "", useCache)
}

// MergeTarget data from finder.Result into aliases map
func (m *Map) MergeTarget(r finder.Result, target string, saveCache bool) []byte {
	var buf bytes.Buffer

	series := r.Series()
	buf.Grow(len(series) * 24)
	for i := 0; i < len(series); i++ {
		if saveCache {
			buf.Write(series[i])
			buf.WriteByte('\n')
		}
		key := string(series[i])
		if len(key) == 0 {
			continue
		}
		abs := string(r.Abs(series[i]))
		m.lock.Lock()
		if x, ok := m.data[key]; ok {
			m.data[key] = append(x, Value{Target: target, DisplayName: abs})
		} else {
			m.data[key] = []Value{{Target: target, DisplayName: abs}}
		}
		m.lock.Unlock()
	}

	if saveCache {
		return buf.Bytes()
	} else {
		return nil
	}
}

// Len returns count of keys
func (m *Map) Len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.data)
}

// Size returns count of values
func (m *Map) Size() int {
	s := 0
	m.lock.RLock()
	defer m.lock.RUnlock()
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

// TODO (msaf1980): may be more effectuve way for select optimal datable type for  best query perfomance
// naive algorithm, based on names prefix intersections length and last node density
func (m *Map) IsReversePrefered(defaultRevOrder bool, minMetrics, revDensity, autoSamples int) bool {
	if len(m.data) < minMetrics || len(m.data) == 0 {
		return defaultRevOrder
	}
	if autoSamples <= 0 {
		autoSamples = len(m.data)
	}

	// var prefix, firstName string
	var uniqLastNodes map[string]struct{}
	n := 0
	for k := range m.data {
		if uniqLastNodes == nil {
			if strings.IndexByte(k, '?') > -1 {
				// tagged
				return defaultRevOrder
			}
			uniqLastNodes = make(map[string]struct{})
			// 	prefix = k
			// 	firstName = k
			// } else {
			// 	prefix = utils.IntersectionPrefix(prefix, k)
		}
		if n >= autoSamples {
			break
		}
		n++
		lastNode := utils.LastNode(k)
		if _, ok := uniqLastNodes[lastNode]; !ok {
			uniqLastNodes[lastNode] = struct{}{}
		}
		if len(uniqLastNodes)*revDensity > len(m.data) {
			// break scan, select direct table
			return false
		}
	}

	// uniq_count_of_last_nodes > metrcis_count / density
	// low density (many uniq count last nodes), use direct tables
	return len(uniqLastNodes)*revDensity <= len(m.data)
}
