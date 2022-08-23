package point

import (
	"fmt"
	"sort"

	"github.com/lomik/graphite-clickhouse/pkg/dry"
)

// Points is a structure that stores points and additional information about them, e.g. steps, aggregating functions and names.
type Points struct {
	list    []Point
	idMap   map[string]uint32
	metrics []string
	steps   []uint32
	aggs    []*string
	uniqAgg []string
}

// NextMetric returns the list of points for one metric name
type NextMetric func() []Point

// NewPoints return new empty Points
func NewPoints() *Points {
	return &Points{
		list:    make([]Point, 0),
		idMap:   make(map[string]uint32),
		metrics: make([]string, 0),
	}
}

// AppendPoint creates a Point with given values and appends it to list
func (pp *Points) AppendPoint(metricID uint32, value float64, time uint32, version uint32) {
	pp.list = append(pp.list, Point{
		MetricID:  metricID,
		Value:     value,
		Time:      time,
		Timestamp: version,
	})
}

// MetricID checks if metric name already exists and returns the ID for it. If not, it creates it first.
func (pp *Points) MetricID(metricName string) uint32 {
	id := pp.idMap[metricName]
	if id == 0 {
		pp.metrics = append(pp.metrics, metricName)
		id = uint32(len(pp.metrics))
		pp.idMap[metricName] = id
	}
	return id
}

// MetricIDBytes checks if metric name already exists and returns the ID for it. If not, it creates it first.
func (pp *Points) MetricIDBytes(metricNameBytes []byte) uint32 {
	return pp.MetricID(dry.UnsafeString(metricNameBytes))
}

// MetricName returns name for metric with given metricID or empty string when ID does not exist
func (pp *Points) MetricName(metricID uint32) string {
	i := int(metricID)
	if i < 1 || len(pp.metrics) < i {
		return ""
	}
	return pp.metrics[i-1]
}

// List returns list of points
func (pp *Points) List() []Point {
	return pp.list
}

// ReplaceList replaces list of points
func (pp *Points) ReplaceList(list []Point) {
	pp.list = list
}

// GetStep returns uint32 step for given metric id.
func (pp *Points) GetStep(id uint32) (uint32, error) {
	i := int(id)
	if i < 1 || len(pp.steps) < i {
		return 0, fmt.Errorf("wrong id %d for given steps %d: %w", i, len(pp.steps), ErrWrongMetricID)
	}
	return pp.steps[i-1], nil
}

// SetSteps accepts map of metric name as keys and step as values and sets slice of uint32 steps for existing metrics in Data.Points
func (pp *Points) SetSteps(steps map[uint32][]string) {
	if len(steps) == 0 {
		return
	}

	pp.steps = make([]uint32, len(pp.metrics))
	for step, mm := range steps {
		for _, m := range mm {
			if id, ok := pp.idMap[m]; ok {
				pp.steps[id-1] = step
			}
		}
	}
}

// GetAggregation returns string function for given metric id.
func (pp *Points) GetAggregation(id uint32) (string, error) {
	i := int(id)
	if i < 1 || len(pp.aggs) < i {
		return "", fmt.Errorf("wrong id %d for given functions %d: %w", i, len(pp.aggs), ErrWrongMetricID)
	}
	return *pp.aggs[i-1], nil
}

// SetAggregations accepts map of metric name as keys and function as values and sets slice of functions for existing metrics in Data.Points
func (pp *Points) SetAggregations(functions map[string][]string) {
	pp.aggs = make([]*string, len(pp.metrics))
	pp.uniqAgg = make([]string, 0, len(functions))
	for f := range functions {
		pp.uniqAgg = append(pp.uniqAgg, f)
	}
	for i, f := range pp.uniqAgg {
		for _, m := range functions[f] {
			if id, ok := pp.idMap[m]; ok {
				pp.aggs[id-1] = &pp.uniqAgg[i]
			}
		}
	}
}

func (pp *Points) Len() int {
	return len(pp.list)
}

func (pp *Points) Less(i, j int) bool {
	if pp.list[i].MetricID == pp.list[j].MetricID {
		return pp.list[i].Time < pp.list[j].Time
	}

	return pp.list[i].MetricID < pp.list[j].MetricID
}

func (pp *Points) Swap(i, j int) {
	pp.list[i], pp.list[j] = pp.list[j], pp.list[i]
}

// Sort sorts the points list by ID, Time
func (pp *Points) Sort() {
	sort.Sort(pp)
}

// Uniq cleans up the points
func (pp *Points) Uniq() {
	pp.list = Uniq(pp.list)
}

// GroupByMetric returns NextMetric function, that by each call returns points for one next metric.
// It should be called only on sorted and cleaned Points.
func (pp *Points) GroupByMetric() NextMetric {
	var i, n int
	l := pp.Len()
	// i - current position of iterator
	// n - position of the first record with current metric
	return func() []Point {
		if n == l {
			return []Point{}
		}
		for i = n; i < l; i++ {
			if pp.list[i].MetricID != pp.list[n].MetricID {
				points := pp.list[n:i]
				n = i
				return points
			}
		}
		points := pp.list[n:i]
		n = i
		return points
	}
}
