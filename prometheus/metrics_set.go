package prometheus

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// SeriesSet contains a set of series.
type metricsSet struct {
	metrics []string
	offset  int
}

type metric struct {
	name string
}

type dummyIterator struct{}

var _ storage.SeriesSet = &metricsSet{}
var _ storage.SeriesIterator = &dummyIterator{}

func (ms *metricsSet) At() storage.Series {
	return &metric{name: ms.metrics[ms.offset]}
}

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (it *dummyIterator) Seek(t int64) bool { return false }

// At returns the current timestamp/value pair.
func (it *dummyIterator) At() (t int64, v float64) { return 0, 0 }

// Next advances the iterator by one.
func (it *dummyIterator) Next() bool { return false }

// Err returns the current error.
func (it *dummyIterator) Err() error { return nil }

// Iterator returns a new iterator of the data of the series.
func (s *metric) Iterator() storage.SeriesIterator {
	return &dummyIterator{}
}

func (s *metric) Labels() labels.Labels {
	u, err := urlParse(s.name)
	if err != nil {
		return labels.Labels{labels.Label{Name: "__name__", Value: s.name}}
	}

	q := u.Query()
	lb := make(labels.Labels, len(q)+1)
	lb[0].Name = "__name__"
	lb[0].Value = u.Path

	i := 0
	for k, v := range q {
		i++
		lb[i].Name = k
		lb[i].Value = v[0]
	}

	return lb
}

// Err returns the current error.
func (ms *metricsSet) Err() error { return nil }

func (ms *metricsSet) Next() bool {
	if ms.offset < 0 {
		ms.offset = 0
	} else {
		ms.offset++
	}

	return ms.offset < len(ms.metrics)
}

func newMetricsSet(metrics []string) storage.SeriesSet {
	return &metricsSet{metrics: metrics, offset: -1}
}
