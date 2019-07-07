package rollup

import (
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/lomik/graphite-clickhouse/pkg/dry"

	"github.com/lomik/graphite-clickhouse/helper/point"
)

type Retention struct {
	Age       uint32 `json:"age"`
	Precision uint32 `json:"precision"`
}

type Pattern struct {
	Regexp    string      `json:"regexp"`
	Function  string      `json:"function"`
	Retention []Retention `json:"retention"`
	aggr      *Aggr
	re        *regexp.Regexp
}

type Rules struct {
	Pattern []Pattern `json:"pattern"`
	Updated int64     `json:"updated"`
}

// should never be used in real conditions
var superDefaultFunction = AggrMap["avg"]

const superDefaultPrecision = uint32(60)

func (p *Pattern) compile() error {
	var err error
	if p.Regexp != "" && p.Regexp != ".*" {
		p.re, err = regexp.Compile(p.Regexp)
		if err != nil {
			return err
		}
	} else {
		p.Regexp = ".*"
		p.re = nil
	}

	if p.Function != "" {
		var exists bool
		p.aggr, exists = AggrMap[p.Function]

		if !exists {
			return fmt.Errorf("unknown function %#v", p.Function)
		}
	}

	if len(p.Retention) > 0 {
		// reverse sort by age
		sort.Slice(p.Retention, func(i, j int) bool { return p.Retention[i].Age < p.Retention[j].Age })
	} else {
		p.Retention = nil
	}

	return nil
}

func (r *Rules) compile() (*Rules, error) {
	if r.Pattern == nil {
		r.Pattern = make([]Pattern, 0)
	}

	for i := range r.Pattern {
		if err := r.Pattern[i].compile(); err != nil {
			return r, err
		}
	}

	return r, nil
}

func (r *Rules) withDefault(defaultPrecision uint32, defaultFunction *Aggr) *Rules {
	patterns := make([]Pattern, len(r.Pattern)+1)
	copy(patterns, r.Pattern)

	var retention []Retention
	if defaultPrecision != 0 {
		retention = []Retention{
			Retention{Age: 0, Precision: defaultPrecision},
		}
	}

	patterns = append(patterns, Pattern{
		Regexp:    ".*",
		Function:  defaultFunction.Name(),
		Retention: retention,
	})
	n, _ := (&Rules{Pattern: patterns, Updated: r.Updated}).compile()
	return n
}

func (r *Rules) setUpdated() *Rules {
	r.Updated = time.Now().Unix()
	return r
}

func (r *Rules) withSuperDefault() *Rules {
	return r.withDefault(superDefaultPrecision, superDefaultFunction)
}

// Lookup returns precision and aggregate function for metric name and age
func (r *Rules) Lookup(metric string, age uint32) (precision uint32, ag *Aggr) {
	precisionFound := false

	for _, p := range r.Pattern {
		// pattern hasn't interested data
		if (ag != nil || p.aggr == nil) && (precisionFound || len(p.Retention) == 0) {
			continue
		}

		// metric not matched regexp
		if p.re != nil && !p.re.MatchString(metric) {
			continue
		}

		if ag == nil && p.aggr != nil {
			ag = p.aggr
		}

		if !precisionFound && len(p.Retention) > 0 {
			for i, r := range p.Retention {
				if age < r.Age {
					if i > 0 {
						precision = p.Retention[i-1].Precision
						precisionFound = true
					}
					break
				}
				if i == len(p.Retention)-1 {
					precision = r.Precision
					precisionFound = true
					break
				}
			}
		}

		// all found
		if ag != nil && precisionFound {
			return
		}
	}

	if ag == nil {
		ag = superDefaultFunction
	}

	if !precisionFound {
		precision = superDefaultPrecision
	}

	return
}

// LookupBytes returns precision and aggregate function for metric name and age
func (r *Rules) LookupBytes(metric []byte, age uint32) (precision uint32, ag *Aggr) {
	return r.Lookup(dry.UnsafeString(metric), age)
}

func doMetricPrecision(points []point.Point, precision uint32, aggr *Aggr) []point.Point {
	l := len(points)
	var i, n int
	// i - current position of iterator
	// n - position of the first record with time rounded to precision

	if l == 0 {
		return points
	}

	// set first point time
	t := points[0].Time
	t = t - (t % precision)
	points[0].Time = t

	for i = 1; i < l; i++ {
		t = points[i].Time
		t = t - (t % precision)
		points[i].Time = t

		if points[n].Time == t {
			points[i].MetricID = 0
		} else {
			if i > n+1 {
				points[n].Value = aggr.Do(points[n:i])
			}
			n = i
		}
	}
	if i > n+1 {
		points[n].Value = aggr.Do(points[n:i])
	}

	return point.CleanUp(points)
}

// RollupMetricAge rolling up list of points of ONE metric sorted by key "time"
// returns (new points slice, precision)
func (r *Rules) RollupMetricAge(metricName string, age uint32, points []point.Point) ([]point.Point, uint32, error) {
	l := len(points)
	if l == 0 {
		return points, 1, nil
	}

	precision, ag := r.Lookup(metricName, age)
	points = doMetricPrecision(points, precision, ag)

	return points, precision, nil
}

// RollupMetric rolling up list of points of ONE metric sorted by key "time"
// returns (new points slice, precision)
func (r *Rules) RollupMetric(metricName string, from uint32, points []point.Point) ([]point.Point, uint32, error) {
	now := uint32(time.Now().Unix())
	age := uint32(0)
	if now > from {
		age = now - from
	}
	return r.RollupMetricAge(metricName, age, points)
}
