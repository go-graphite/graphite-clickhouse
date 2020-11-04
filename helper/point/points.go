package point

import (
	"fmt"
	"sort"
)

type Points struct {
	list    []Point
	idMap   map[string]uint32
	metrics []string
	steps   []uint32
	aggs    []*string
	uniqAgg []string
}

func NewPoints() *Points {
	return &Points{
		list:    make([]Point, 0),
		idMap:   make(map[string]uint32),
		metrics: make([]string, 0),
	}
}

func (pp *Points) AppendPoint(metricID uint32, value float64, time uint32, version uint32) {
	pp.list = append(pp.list, Point{
		MetricID:  metricID,
		Value:     value,
		Time:      time,
		Timestamp: version,
	})
}

func (pp *Points) MetricID(metricName string) uint32 {
	id := pp.idMap[metricName]
	if id == 0 {
		pp.metrics = append(pp.metrics, metricName)
		id = uint32(len(pp.metrics))
		pp.idMap[metricName] = id
	}
	return id
}

func (pp *Points) MetricIDBytes(metricNameBytes []byte) uint32 {
	// @TODO: optimize?
	return pp.MetricID(string(metricNameBytes))
}

func (pp *Points) MetricName(metricID uint32) string {
	i := int(metricID)
	if i < 1 || len(pp.metrics) < i {
		return ""
	}
	return pp.metrics[i-1]
}

func (pp *Points) List() []Point {
	return pp.list
}

func (pp *Points) ReplaceList(list []Point) {
	pp.list = list
}

// GetStep returns uint32 step for given metric id.
func (pp *Points) GetStep(id uint32) (uint32, error) {
	i := int(id)
	if i < 1 || len(pp.steps) < i {
		return 0, fmt.Errorf("wrong id %v for given steps: %v", i, len(pp.steps))
	}
	return pp.steps[i-1], nil
}

// SetSteps accepts map of metric name as keys and step as values and sets slice of uint32 steps for existing metrics in Data.Points
func (pp *Points) SetSteps(steps map[string]uint32) {
	pp.steps = make([]uint32, len(pp.metrics))
	for m, step := range steps {
		if id, ok := pp.idMap[m]; ok {
			pp.steps[id-1] = step
		}
	}
}

// GetAggregation returns string function for given metric id.
func (pp *Points) GetAggregation(id uint32) (string, error) {
	i := int(id)
	if i < 1 || len(pp.aggs) < i {
		return "", fmt.Errorf("wrong id %v for given functions: %v", i, len(pp.aggs))
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

func (pp *Points) Sort() {
	sort.Sort(pp)
}

func (pp *Points) Uniq() {
	pp.list = Uniq(pp.list)
}
