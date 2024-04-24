package rollup

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/pkg/dry"

	"github.com/lomik/graphite-clickhouse/helper/point"
)

type Retention struct {
	Age       uint32 `json:"age"`
	Precision uint32 `json:"precision"`
}

type RuleType uint8

const (
	RuleAll RuleType = iota
	RulePlain
	RuleTagged
	RuleTagList
)

var timeNow = time.Now

var ruleTypeStrings []string = []string{"all", "plain", "tagged", "tag_list"}

func (r *RuleType) String() string {
	return ruleTypeStrings[*r]
}

func (r *RuleType) Set(value string) error {
	switch strings.ToLower(value) {
	case "all":
		*r = RuleAll
	case "plain":
		*r = RulePlain
	case "tagged":
		*r = RuleTagged
	case "tag_list":
		*r = RuleTagList
	default:
		return fmt.Errorf("invalid rule type %s", value)
	}
	return nil
}

func (r *RuleType) UnmarshalJSON(data []byte) error {
	s := string(data)
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		s = s[1 : len(s)-1]
	}
	return r.Set(s)
}

func (r *RuleType) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	return r.Set(s)
}

func splitTags(tagsStr string) (tags []string) {
	vals := strings.Split(tagsStr, ";")
	tags = make([]string, 0, len(vals))
	// remove empthy elements
	for _, v := range vals {
		if v != "" {
			tags = append(tags, v)
		}
	}
	return
}

func buildTaggedRegex(regexpStr string) string {
	// see buildTaggedRegex in https://github.com/ClickHouse/ClickHouse/blob/780a1b2abea918d3205d149db7689a31fdff2f70/src/Processors/Merges/Algorithms/Graphite.cpp#L241
	//
	// * tags list in format (for name or any value can use regexp, alphabet sorting not needed)
	// * spaces are not stiped and used as tag and value part
	// * name must be first (if used)
	// *
	// * tag1=value1; tag2=VALUE2_REGEX;tag3=value3
	// * or
	// * name;tag1=value1;tag2=VALUE2_REGEX;tag3=value3
	// * or for one tag
	// * tag1=value1
	// *
	// * Resulting regex against metric like
	// * name?tag1=value1&tag2=value2
	// *
	// * So,
	// *
	// * name
	// * produce
	// * name\?
	// *
	// * tag2=val2
	// * produce
	// * [\?&]tag2=val2(&.*)?$
	// *
	// * nam.* ; tag1=val1 ; tag2=val2
	// * produce
	// * nam.*\?(.*&)?tag1=val1&(.*&)?tag2=val2(&.*)?$

	tags := splitTags(regexpStr)

	if strings.Contains(tags[0], "=") {
		regexpStr = "[\\?&]"
	} else {
		if len(tags) == 1 {
			// only name
			return "^" + tags[0] + "\\?"
		}
		// start with name value
		regexpStr = "^" + tags[0] + "\\?(.*&)?"
		tags = tags[1:]
	}

	sort.Strings(tags) // sorted tag keys
	regexpStr = regexpStr +
		strings.Join(tags, "&(.*&)?") +
		"(&.*)?$" // close regex

	return regexpStr
}

type Pattern struct {
	RuleType  RuleType    `json:"rule_type"`
	Regexp    string      `json:"regexp"`
	Function  string      `json:"function"`
	Retention []Retention `json:"retention"`
	aggr      *Aggr
	re        *regexp.Regexp
}

type Rules struct {
	Pattern       []Pattern `json:"pattern"`
	Updated       int64     `json:"updated"`
	Splitted      bool      `json:"-"`
	PatternPlain  []Pattern `json:"-"`
	PatternTagged []Pattern `json:"-"`
}

// NewMockRulles creates mock rollup for tests
func NewMockRules(pattern []Pattern, defaultPrecision uint32, defaultFunction string) (*Rules, error) {
	rules, err := (&Rules{Pattern: pattern}).compile()
	if err != nil {
		return nil, err
	}
	rules, err = rules.prepare(defaultPrecision, defaultFunction)
	if err != nil {
		return nil, err
	}
	return rules, nil
}

// should never be used in real conditions
var superDefaultFunction = AggrMap["avg"]

const superDefaultPrecision = uint32(60)

func (p *Pattern) compile() error {
	if p.RuleType == RuleTagList {
		// convert to tagged rule type
		p.RuleType = RuleTagged
		p.Regexp = buildTaggedRegex(p.Regexp)
	}

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
	r.PatternPlain = make([]Pattern, 0)
	r.PatternTagged = make([]Pattern, 0)

	r.Splitted = false
	for i := range r.Pattern {
		if err := r.Pattern[i].compile(); err != nil {
			return r, err
		}
		if !r.Splitted && r.Pattern[i].RuleType != RuleAll {
			r.Splitted = true
		}
	}

	if r.Splitted {
		for i := range r.Pattern {
			switch r.Pattern[i].RuleType {
			case RulePlain:
				r.PatternPlain = append(r.PatternPlain, r.Pattern[i])
			case RuleTagged:
				r.PatternTagged = append(r.PatternTagged, r.Pattern[i])
			default:
				r.PatternPlain = append(r.PatternPlain, r.Pattern[i])
				r.PatternTagged = append(r.PatternTagged, r.Pattern[i])
			}
		}
	}

	return r, nil
}

func (r *Rules) prepare(defaultPrecision uint32, defaultFunction string) (*Rules, error) {
	defaultAggr := AggrMap[defaultFunction]
	if defaultFunction != "" && defaultAggr == nil {
		return r, fmt.Errorf("unknown function %#v", defaultFunction)
	}
	return r.withDefault(defaultPrecision, defaultAggr).withSuperDefault().setUpdated(), nil
}

func (r *Rules) withDefault(defaultPrecision uint32, defaultFunction *Aggr) *Rules {
	patterns := make([]Pattern, len(r.Pattern)+1)
	copy(patterns, r.Pattern)

	var retention []Retention
	if defaultPrecision != 0 {
		retention = []Retention{{Age: 0, Precision: defaultPrecision}}
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
	r.Updated = timeNow().Unix()
	return r
}

func (r *Rules) withSuperDefault() *Rules {
	return r.withDefault(superDefaultPrecision, superDefaultFunction)
}

// Lookup returns precision and aggregate function for metric name and age
func (r *Rules) Lookup(metric string, age uint32, verbose bool) (precision uint32, ag *Aggr, aggrPattern, retentionPattern *Pattern) {
	if r.Splitted {
		if strings.Contains(metric, "?") {
			return lookup(metric, age, r.PatternTagged, verbose)
		}
		return lookup(metric, age, r.PatternPlain, verbose)
	}
	return lookup(metric, age, r.Pattern, verbose)
}

// Lookup returns precision and aggregate function for metric name and age
func lookup(metric string, age uint32, patterns []Pattern, verbose bool) (precision uint32, ag *Aggr, aggrPattern, retentionPattern *Pattern) {
	precisionFound := false

	for _, p := range patterns {
		// pattern hasn't interested data
		if (ag != nil || p.aggr == nil) && (precisionFound || len(p.Retention) == 0) {
			continue
		}

		// metric not matched regexp
		if p.re != nil && !p.re.MatchString(metric) {
			continue
		}

		if ag == nil && p.aggr != nil {
			if verbose {
				aggrPattern = &p
			}
			ag = p.aggr
		}

		if !precisionFound && len(p.Retention) > 0 {
			for i, r := range p.Retention {
				if age < r.Age {
					if i > 0 {
						precision = p.Retention[i-1].Precision
						precisionFound = true
						if verbose {
							retentionPattern = &p
						}
					}
					break
				}
				if i == len(p.Retention)-1 {
					precision = r.Precision
					precisionFound = true
					if verbose {
						retentionPattern = &p
					}
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
func (r *Rules) LookupBytes(metric []byte, age uint32, verbose bool) (precision uint32, ag *Aggr, aggrPattern, retentionPattern *Pattern) {
	return r.Lookup(dry.UnsafeString(metric), age, verbose)
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

	precision, ag, _, _ := r.Lookup(metricName, age, false)
	points = doMetricPrecision(points, precision, ag)

	return points, precision, nil
}

// RollupMetric rolling up list of points of ONE metric sorted by key "time"
// returns (new points slice, precision)
func (r *Rules) RollupMetric(metricName string, from uint32, points []point.Point) ([]point.Point, uint32, error) {
	now := uint32(timeNow().Unix())
	age := uint32(0)
	if now > from {
		age = now - from
	}
	return r.RollupMetricAge(metricName, age, points)
}

// RollupPoints groups sorted Points by metric name and apply rollup one by one.
// If the `step` parameter is 0, it will be got from the current *Rules, otherwise it will be used directly.
func (r *Rules) RollupPoints(pp *point.Points, from int64, step int64) error {
	if from < 0 || step < 0 {
		return fmt.Errorf("from and step must be >= 0: %v, %v", from, step)
	}

	now := int64(timeNow().Unix())
	age := int64(0)
	if now > from {
		age = now - from
	}

	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := pp.Len()
	if l == 0 {
		return nil
	}
	oldPoints := pp.List()
	newPoints := make([]point.Point, 0, pp.Len())
	rollup := func(p []point.Point) ([]point.Point, error) {
		metricName := pp.MetricName(p[0].MetricID)
		var err error
		if step == 0 {
			p, _, err = r.RollupMetricAge(metricName, uint32(age), p)
		} else {
			_, agg, _, _ := r.Lookup(metricName, uint32(from), false)
			p = doMetricPrecision(p, uint32(step), agg)
		}
		for i := range p {
			p[i].MetricID = p[0].MetricID
		}
		return p, err
	}

	for i = 1; i < l; i++ {
		if oldPoints[i].MetricID != oldPoints[n].MetricID {
			points, err := rollup(oldPoints[n:i])
			if err != nil {
				return err
			}
			newPoints = append(newPoints, points...)
			n = i
			continue
		}
	}

	points, err := rollup(oldPoints[n:i])
	if err != nil {
		return err
	}
	newPoints = append(newPoints, points...)
	pp.ReplaceList(newPoints)
	return nil
}
