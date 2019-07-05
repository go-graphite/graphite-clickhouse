package prometheus

import (
	"net/url"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/helper/rollup"

	"github.com/lomik/graphite-clickhouse/render"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
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

func makeSeries(metricName string, points []point.Point, rollupRules *rollup.Rules) (series, error) {
	points, _, err := rollupRules.RollupMetric(metricName, points[0].Time, points)
	if err != nil {
		return series{}, err
	}

	return series{metricName: metricName, points: points}, nil
}

func makeSeriesSet(data *render.Data, rollupRules *rollup.Rules) (storage.SeriesSet, error) {
	ss := &seriesSet{series: make([]series, 0), current: -1}
	if data == nil {
		return ss, nil
	}

	points := data.Points.List()

	if len(points) == 0 {
		return ss, nil
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric

	for i = 1; i < len(points); i++ {
		if points[i].MetricID != points[n].MetricID {
			s, err := makeSeries(data.Points.MetricName(points[n].MetricID), points[n:i], rollupRules)
			if err != nil {
				return ss, err
			}
			ss.series = append(ss.series, s)
			n = i
			continue
		}
	}

	s, err := makeSeries(data.Points.MetricName(points[n].MetricID), points[n:i], rollupRules)
	if err != nil {
		return ss, err
	}
	ss.series = append(ss.series, s)

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
func (sit *seriesIterator) Seek(t int64) bool {
	tt := uint32(t / 1000)
	if t%1000 != 0 {
		tt++
	}

	for ; sit.current < len(sit.points); sit.current++ {
		if sit.points[sit.current].Time >= tt {
			// sit.logger().Debug("seriesIterator.Seek", zap.Int64("t", t), zap.Bool("ret", true))
			return true
		}
	}

	// sit.logger().Debug("seriesIterator.Seek", zap.Int64("t", t), zap.Bool("ret", false))
	return false
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

// Next advances the iterator by one.
func (sit *seriesIterator) Next() bool {
	if sit.current+1 < len(sit.points) {
		sit.current++
		// sit.logger().Debug("seriesIterator.Next", zap.Bool("ret", true))
		return true
	}
	// sit.logger().Debug("seriesIterator.Next", zap.Bool("ret", false))
	return false
}

// Err returns the current error.
func (sit *seriesIterator) Err() error { return nil }

// Err returns the current error.
func (ss *seriesSet) Err() error { return nil }

func urlParse(rawurl string) (*url.URL, error) {
	p := strings.IndexByte(rawurl, '?')
	if p < 0 {
		return url.Parse(rawurl)
	}
	m, err := url.Parse(rawurl[p:])
	if m != nil {
		m.Path = rawurl[:p]
	}
	return m, err
}

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

// Iterator returns a new iterator of the data of the series.
func (s *series) Iterator() storage.SeriesIterator {
	return &seriesIterator{metricName: s.metricName, points: s.points, current: -1}
}

func (s *series) name() string {
	return s.metricName
}

func (s *series) Labels() labels.Labels {
	metricName := s.name()

	u, err := urlParse(metricName)
	if err != nil {
		return labels.Labels{labels.Label{Name: "__name__", Value: metricName}}
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
