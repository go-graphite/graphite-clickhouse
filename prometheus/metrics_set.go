//go:build !noprom
// +build !noprom

package prometheus

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// SeriesSet contains a set of series.
type metricsSet struct {
	metrics []string
	current int
}

type metric struct {
	name string
}

var _ storage.SeriesSet = &metricsSet{}

func (ms *metricsSet) At() storage.Series {
	return &metric{name: ms.metrics[ms.current]}
}

// Iterator returns a new iterator of the data of the series.
func (s *metric) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	return emptyIteratorValue
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

func newMetricsSet(metrics []string) storage.SeriesSet {
	return &metricsSet{metrics: metrics, current: -1}
}

// Warnings ...
func (s *metricsSet) Warnings() annotations.Annotations { return nil }
