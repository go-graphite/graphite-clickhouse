//go:build !noprom
// +build !noprom

package prometheus

import (
	"github.com/prometheus/prometheus/util/annotations"
	"log"

	"github.com/lomik/graphite-clickhouse/helper/point"

	"github.com/lomik/graphite-clickhouse/render/data"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// SeriesIterator iterates over the data of a time series.
type seriesIterator struct {
	metricName string
	points     []point.Point
	current    int
}

// Series represents a single time series.
type series struct {
	metricName string
	points     []point.Point
}

// SeriesSet contains a set of series.
type seriesSet struct {
	series  []series
	current int
}

var _ storage.SeriesSet = &seriesSet{}

func makeSeriesSet(data *data.Data) (storage.SeriesSet, error) {
	ss := &seriesSet{series: make([]series, 0), current: -1}
	if data == nil {
		return ss, nil
	}

	if data.Len() == 0 {
		return ss, nil
	}

	nextMetric := data.GroupByMetric()
	for {
		points := nextMetric()
		if len(points) == 0 {
			break
		}

		metricName := data.MetricName(points[0].MetricID)
		for _, v := range data.AM.Get(metricName) {
			ss.series = append(ss.series, series{metricName: v.DisplayName, points: points})
		}
	}

	return ss, nil
}

func emptySeriesSet() storage.SeriesSet {
	return &seriesSet{series: make([]series, 0), current: -1}
}

// func (sit *seriesIterator) logger() *zap.Logger {
// 	return zap.L() //.With(zap.String("metric", sit.metricName))
// }

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (sit *seriesIterator) Seek(t int64) chunkenc.ValueType {
	tt := uint32(t / 1000)
	if t%1000 != 0 {
		tt++
	}

	for ; sit.current < len(sit.points); sit.current++ {
		if sit.points[sit.current].Time >= tt {
			// sit.logger().Debug("seriesIterator.Seek", zap.Int64("t", t), zap.Bool("ret", true))
			return chunkenc.ValFloat
		}
	}

	// sit.logger().Debug("seriesIterator.Seek", zap.Int64("t", t), zap.Bool("ret", false))
	return chunkenc.ValNone
}

// At returns the current timestamp/value pair.
func (sit *seriesIterator) At() (t int64, v float64) {
	index := sit.current
	if index < 0 || index >= len(sit.points) {
		index = 0
	}
	p := sit.points[index]
	// sit.logger().Debug("seriesIterator.At", zap.Int64("t", int64(p.Time)*1000), zap.Float64("v", p.Value))
	return int64(p.Time) * 1000, p.Value
}

// AtHistogram returns the current timestamp/value pair if the value is
// a histogram with integer counts. Before the iterator has advanced,
// the behaviour is unspecified.
func (sit *seriesIterator) AtHistogram(histogram *histogram.Histogram) (int64, *histogram.Histogram) {
	log.Fatal("seriesIterator.AtHistogram not implemented")
	return 0, nil // @TODO
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behaviour is unspecified.
func (sit *seriesIterator) AtFloatHistogram(histogram *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	log.Fatal("seriesIterator.AtFloatHistogram not implemented")
	return 0, nil // @TODO
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behaviour is unspecified.
func (sit *seriesIterator) AtT() int64 {
	t, _ := sit.At()
	return t
}

// Next advances the iterator by one.
func (sit *seriesIterator) Next() chunkenc.ValueType {
	if sit.current+1 < len(sit.points) {
		sit.current++
		// sit.logger().Debug("seriesIterator.Next", zap.Bool("ret", true))
		return chunkenc.ValFloat
	}
	// sit.logger().Debug("seriesIterator.Next", zap.Bool("ret", false))
	return chunkenc.ValNone
}

// Err returns the current error.
func (sit *seriesIterator) Err() error { return nil }

// Err returns the current error.
func (ss *seriesSet) Err() error { return nil }

func (ss *seriesSet) At() storage.Series {
	if ss == nil || ss.current < 0 || ss.current >= len(ss.series) {
		// zap.L().Debug("seriesSet.At", zap.String("metricName", "nil"))
		return nil
	}
	s := &ss.series[ss.current]
	// zap.L().Debug("seriesSet.At", zap.String("metricName", s.name()))
	return s
}

func (ss *seriesSet) Next() bool {
	if ss == nil || ss.current+1 >= len(ss.series) {
		// zap.L().Debug("seriesSet.Next", zap.Bool("ret", false))
		return false
	}

	ss.current++
	// zap.L().Debug("seriesSet.Next", zap.Bool("ret", true))
	return true
}

// Warnings ...
func (s *seriesSet) Warnings() annotations.Annotations {
	return nil
}

// Iterator returns a new iterator of the data of the series.
func (s *series) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	return &seriesIterator{metricName: s.metricName, points: s.points, current: -1}
}

func (s *series) name() string {
	return s.metricName
}

func (s *series) Labels() labels.Labels {
	return Labels(s.name())
}
