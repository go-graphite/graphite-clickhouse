package rollup

import (
	"github.com/lomik/graphite-clickhouse/helper/point"
)

var AggrMap = map[string]*Aggr{
	"avg":     &Aggr{"avg", AggrAvg},
	"max":     &Aggr{"max", AggrMax},
	"min":     &Aggr{"min", AggrMin},
	"sum":     &Aggr{"sum", AggrSum},
	"any":     &Aggr{"any", AggrAny},
	"anyLast": &Aggr{"anyLast", AggrAnyLast},
}

type Aggr struct {
	name string
	f    func(points []point.Point) (r float64)
}

func (ag *Aggr) Name() string {
	if ag == nil {
		return ""
	}
	return ag.name
}

func (ag *Aggr) String() string {
	if ag == nil {
		return ""
	}
	return ag.name
}

func (ag *Aggr) Do(points []point.Point) (r float64) {
	if ag == nil || ag.f == nil {
		return 0
	}
	return ag.f(points)
}

func AggrSum(points []point.Point) (r float64) {
	for _, p := range points {
		r += p.Value
	}
	return
}

func AggrMax(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[0].Value
	}
	for _, p := range points {
		if p.Value > r {
			r = p.Value
		}
	}
	return
}

func AggrMin(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[0].Value
	}
	for _, p := range points {
		if p.Value < r {
			r = p.Value
		}
	}
	return
}

func AggrAvg(points []point.Point) (r float64) {
	if len(points) == 0 {
		return
	}
	r = AggrSum(points) / float64(len(points))
	return
}

func AggrAny(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[0].Value
	}
	return
}

func AggrAnyLast(points []point.Point) (r float64) {
	if len(points) > 0 {
		r = points[len(points)-1].Value
	}
	return
}
