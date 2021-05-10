// +build !noprom

package prometheus

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// SeriesSet contains a set of series.
type metricsSet struct {
	metrics  []string
	current  int
	warnings storage.Warnings
}

type metric struct {
	name string
}

type chunkMetricsSet struct {
	metricsSet
}

type dummyIterator struct{}

func (ms *metricsSet) At() storage.Series {
	return &metric{name: ms.metrics[ms.current]}
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
func (s *metric) Iterator() chunks.Iterator {
	return &dummyIterator{}
}

func (s *metric) Labels() labels.Labels {
	return Labels(s.name)
}

// Err returns the current error.
func (ms *metricsSet) Err() error { return nil }

func (ms *metricsSet) Next() bool {
	if ms.current < 0 {
		ms.current = 0
	} else {
		ms.current++
	}

	return ms.current < len(ms.metrics)
}

func (ms *metricsSet) Warnings() storage.Warnings { return ms.warnings }

func newMetricsSet(metrics []string) *metricsSet {
	return &metricsSet{metrics: metrics, current: -1}
}

func (ms *metricsSet) toChunkSeriesSet() storage.ChunkSeriesSet {
	return &chunkMetricsSet{*ms}
}

func (ms *chunkMetricsSet) At() storage.ChunkSeries {
	return ms.metricsSet.At()
}
