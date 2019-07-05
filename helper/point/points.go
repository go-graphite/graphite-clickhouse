package point

import "sort"

type Points struct {
	list    []Point
	idMap   map[string]uint32
	metrics []string
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
	if i > len(pp.metrics) || i < 1 {
		return ""
	}
	return pp.metrics[i-1]
}

func (pp *Points) List() []Point {
	return pp.list
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
