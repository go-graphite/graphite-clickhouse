package rollup

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/point"
)

/*
<graphite_rollup>
 	<pattern>
 		<regexp>click_cost</regexp>
 		<function>any</function>
 		<retention>
 			<age>0</age>
 			<precision>3600</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>60</precision>
 		</retention>
 	</pattern>
 	<default>
 		<function>max</function>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>3600</age>
 			<precision>300</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
 	</default>
</graphite_rollup>
*/

type Retention struct {
	Age       int32 `xml:"age"`
	Precision int32 `xml:"precision"`
}

type Pattern struct {
	Regexp    string                      `xml:"regexp"`
	Function  string                      `xml:"function"`
	Retention []*Retention                `xml:"retention"`
	aggr      func([]point.Point) float64 `xml:"-"`
	re        *regexp.Regexp              `xml:"-"`
}

type Rollup struct {
	Pattern []*Pattern `xml:"pattern"`
	Default *Pattern   `xml:"default"`
}

type ClickhouseRollup struct {
	Rollup Rollup `xml:"graphite_rollup"`
}

func (rr *Pattern) compile(hasRegexp bool) error {
	var err error
	if hasRegexp {
		rr.re, err = regexp.Compile(rr.Regexp)
		if err != nil {
			return err
		}
	}

	aggrMap := map[string](func([]point.Point) float64){
		"avg":     AggrAvg,
		"max":     AggrMax,
		"min":     AggrMin,
		"sum":     AggrSum,
		"any":     AggrAny,
		"anyLast": AggrAnyLast,
	}

	var exists bool
	rr.aggr, exists = aggrMap[rr.Function]

	if !exists {
		return fmt.Errorf("unknown function %#v", rr.Function)
	}

	return nil
}

func (r *Rollup) compile() error {
	if r.Pattern == nil {
		r.Pattern = make([]*Pattern, 0)
	}

	if r.Default == nil {
		return fmt.Errorf("default rollup rule not set")
	}

	if err := r.Default.compile(false); err != nil {
		return err
	}

	for _, rr := range r.Pattern {
		if err := rr.compile(true); err != nil {
			return err
		}
	}

	return nil
}

func ParseXML(body []byte) (*Rollup, error) {
	r := &Rollup{}
	err := xml.Unmarshal(body, r)
	if err != nil {
		return nil, err
	}

	// Maybe we've got Clickhouse's graphite.xml?
	if r.Default == nil && r.Pattern == nil {
		y := &ClickhouseRollup{}
		err = xml.Unmarshal(body, y)
		if err != nil {
			return nil, err
		}
		r = &y.Rollup
	}

	err = r.compile()
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Match returns rollup rules for metric
func (r *Rollup) Match(metric string) *Pattern {
	for _, rr := range r.Pattern {
		if rr.re.MatchString(metric) {
			return rr
		}
	}

	return r.Default
}

func (r *Rollup) Step(metric string, from int32) int32 {
	pattern := r.Match(metric)
	now := int32(time.Now().Unix())

	for i := range pattern.Retention {
		if i == len(pattern.Retention)-1 || from > now-pattern.Retention[i+1].Age {
			return pattern.Retention[i].Precision
		}
	}
	return pattern.Retention[len(pattern.Retention)-1].Precision
}

func doMetricPrecision(points []point.Point, precision int32, aggr func([]point.Point) float64) []point.Point {
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
			points[i].Metric = ""
		} else {
			if i > n+1 {
				points[n].Value = aggr(points[n:i])
			}
			n = i
		}
	}
	if i > n+1 {
		points[n].Value = aggr(points[n:i])
	}

	return point.CleanUp(points)
}

// RollupMetric rolling up list of points of ONE metric sorted by key "time"
// returns (new points slice, precision)
func (r *Rollup) RollupMetric(points []point.Point) ([]point.Point, int32) {
	// pp.Println(points)

	l := len(points)
	if l == 0 {
		return points, 1
	}

	now := int32(time.Now().Unix())
	rule := r.Match(points[0].Metric)
	precision := int32(1)

	for _, retention := range rule.Retention {
		if points[0].Time > now-retention.Age && retention.Age != 0 {
			break
		}

		points = doMetricPrecision(points, retention.Precision, rule.aggr)
		precision = retention.Precision
	}

	// pp.Println(points)
	return points, precision
}
