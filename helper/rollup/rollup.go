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
	Age       uint32 `xml:"age"`
	Precision uint32 `xml:"precision"`
}

type Pattern struct {
	Regexp    string         `xml:"regexp"`
	Function  string         `xml:"function"`
	Retention []*Retention   `xml:"retention"`
	aggr      *Aggr          `xml:"-"`
	re        *regexp.Regexp `xml:"-"`
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

	if rr.Function != "" {
		var exists bool
		rr.aggr, exists = AggrMap[rr.Function]

		if !exists {
			return fmt.Errorf("unknown function %#v", rr.Function)
		}
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
func (r *Rollup) Match(metric string) (*Aggr, []*Retention) {
	var ag *Aggr
	var rt []*Retention

	for _, p := range r.Pattern {
		if p.re.MatchString(metric) {
			if ag == nil && p.aggr != nil {
				ag = p.aggr
			}
			if len(rt) == 0 && len(p.Retention) > 0 {
				rt = p.Retention
			}

			if ag != nil && len(rt) > 0 {
				return ag, rt
			}
		}
	}

	if ag == nil {
		ag = r.Default.aggr
	}
	if len(rt) == 0 {
		rt = r.Default.Retention
	}

	return ag, rt
}

func (r *Rollup) Step(metric string, from uint32) (uint32, error) {
	_, rt := r.Match(metric)
	now := uint32(time.Now().Unix())

	if len(rt) == 0 {
		return 0, fmt.Errorf("rollup retention not found for metric %#v", metric)
	}

	for i := range rt {
		if i == len(rt)-1 || from+rt[i+1].Age > now {
			return rt[i].Precision, nil
		}
	}
	return rt[len(rt)-1].Precision, nil
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

// RollupMetric rolling up list of points of ONE metric sorted by key "time"
// returns (new points slice, precision)
func (r *Rollup) RollupMetric(metricName string, fromTimestamp uint32, points []point.Point) ([]point.Point, uint32, error) {
	// pp.Println(points)

	l := len(points)
	if l == 0 {
		return points, 1, nil
	}

	now := uint32(time.Now().Unix())
	ag, rt := r.Match(metricName)
	precision := uint32(1)

	if len(rt) == 0 {
		return points, 0, fmt.Errorf("rollup retention not found for metric %#v", metricName)
	}
	if ag == nil {
		return points, 0, fmt.Errorf("rollup function not found for metric %#v", metricName)
	}

	for _, retention := range rt {
		if fromTimestamp+retention.Age > now && retention.Age != 0 {
			break
		}

		points = doMetricPrecision(points, retention.Precision, ag)
		precision = retention.Precision
	}

	// pp.Println(points)
	return points, precision, nil
}
