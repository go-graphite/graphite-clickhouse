package data

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
	"github.com/stretchr/testify/assert"
)

func genPattern(regexp, function string, retention []rollup.Retention) rollup.Pattern {
	return rollup.Pattern{Regexp: regexp, Function: function, Retention: retention}
}

var finderResult *finder.MockFinder = finder.NewMockFinder([][]byte{
	[]byte("5_sec.name.max"),
	[]byte("1_min.name.avg"),
	[]byte("5_min.name.min"),
	[]byte("10_min.name.any"), // defaults will be used in rollup.Rules
})

func newAM() *alias.Map {
	am := alias.New()
	am.MergeTarget(finderResult, "*.name.*", false)
	return am
}

func newRules(reversed bool) *rollup.Rules {
	fiveSec := []rollup.Retention{{Age: 0, Precision: 5}, {Age: 3600, Precision: 60}}
	oneMin := []rollup.Retention{{Age: 0, Precision: 60}, {Age: 3600, Precision: 300}}
	fiveMin := []rollup.Retention{{Age: 0, Precision: 300}, {Age: 3600, Precision: 1200}}
	emptyRet := make([]rollup.Retention, 0)
	var pattern []rollup.Pattern
	if reversed {
		pattern = []rollup.Pattern{
			genPattern("[.]5_sec$", "", fiveSec),
			genPattern("[.]1_min$", "", oneMin),
			genPattern("[.]5_min$", "", fiveMin),
			genPattern("^max[.]", "max", emptyRet),
			genPattern("^min[.]", "min", emptyRet),
			genPattern("^avg[.]", "avg", emptyRet),
		}
	} else {
		pattern = []rollup.Pattern{
			genPattern("^5_sec[.]", "", fiveSec),
			genPattern("^1_min[.]", "", oneMin),
			genPattern("^5_min[.]", "", fiveMin),
			genPattern("[.]max$", "max", emptyRet),
			genPattern("[.]min$", "min", emptyRet),
			genPattern("[.]avg$", "avg", emptyRet),
		}
	}
	rules, _ := rollup.NewMockRules(pattern, 30, "avg")
	return rules
}

func ageToTimestamp(age int64) int64 {
	return time.Now().Unix() - age
}

// fromAge and untilAge are relative age of timeframe
func newCondition(fromAge, untilAge, maxDataPoints int64) *conditions {
	tf := TimeFrame{ageToTimestamp(fromAge), ageToTimestamp(untilAge), maxDataPoints}
	tt := NewTargets([]string{"*.name.*"}, newAM())
	tt.pointsTable = "graphite.data"
	tt.rollupRules = newRules(false)
	return &conditions{TimeFrame: &tf, Targets: tt}
}

func extTableString(et map[string]*strings.Builder) map[string]string {
	ett := make(map[string]string)
	for a := range et {
		ett[a] = et[a].String()
	}
	return ett
}

func TestPrepareMetricsLists(t *testing.T) {
	t.Run("unreversed request", func(t *testing.T) {
		cond := newCondition(0, 0, 60)
		cond.isReverse = false
		cond.rollupUseReverted = false
		cond.prepareMetricsLists()
		expectedSeries := finderResult.Strings()
		sort.Strings(expectedSeries)
		sort.Strings(cond.metricsLookup)
		sort.Strings(cond.metricsRequested)
		sort.Strings(cond.metricsUnreverse)
		assert.Equal(t, expectedSeries, cond.metricsUnreverse)
		assert.Equal(t, cond.metricsRequested, cond.metricsUnreverse)
		assert.Equal(t, cond.metricsLookup, cond.metricsRequested)

		// nothing should change in case of cond.isReverse == false
		cond.rollupUseReverted = true
		cond.prepareMetricsLists()
		sort.Strings(cond.metricsLookup)
		sort.Strings(cond.metricsRequested)
		sort.Strings(cond.metricsUnreverse)
		assert.Equal(t, expectedSeries, cond.metricsUnreverse)
		assert.Equal(t, cond.metricsRequested, cond.metricsUnreverse)
		assert.Equal(t, cond.metricsLookup, cond.metricsRequested)
	})

	t.Run("reversed request", func(t *testing.T) {
		cond := newCondition(0, 0, 60)
		cond.isReverse = true
		cond.rollupUseReverted = false
		cond.prepareMetricsLists()
		for i := range cond.metricsRequested {
			assert.Equal(t, cond.metricsRequested[i], reverse.String(cond.metricsUnreverse[i]))
		}
		expectedSeries := finderResult.Strings()
		sort.Strings(expectedSeries)
		expectedSeriesReversed := make([]string, len(expectedSeries))
		for i := range expectedSeries {
			expectedSeriesReversed[i] = reverse.String(expectedSeries[i])
		}
		sort.Strings(expectedSeriesReversed)
		sort.Strings(cond.metricsLookup)
		sort.Strings(cond.metricsRequested)
		sort.Strings(cond.metricsUnreverse)
		assert.Equal(t, expectedSeries, cond.metricsUnreverse)
		assert.Equal(t, expectedSeriesReversed, cond.metricsRequested)
		assert.Equal(t, cond.metricsLookup, cond.metricsRequested)

		cond.rollupUseReverted = true
		cond.prepareMetricsLists()
		for i := range cond.metricsRequested {
			assert.Equal(t, cond.metricsRequested[i], reverse.String(cond.metricsUnreverse[i]))
		}
		sort.Strings(cond.metricsLookup)
		sort.Strings(cond.metricsRequested)
		sort.Strings(cond.metricsUnreverse)
		assert.Equal(t, expectedSeries, cond.metricsUnreverse)
		assert.Equal(t, expectedSeriesReversed, cond.metricsRequested)
		assert.Equal(t, cond.metricsLookup, cond.metricsUnreverse)
	})
}

func TestPrepareLookup(t *testing.T) {
	// cases:
	//  - aggregater / non-aggregated
	//  - proper/inproper lookup rules for reversed table
	// testing:
	//  - c.aggregations
	//  - c.extDataBodies: only for c.isReverse=false
	//  - c.steps
	t.Run("aggregated non-reverse query", func(t *testing.T) {
		cond := newCondition(5400, 1800, 5)
		cond.aggregated = true
		cond.isReverse = false
		cond.prepareMetricsLists()
		sort.Strings(cond.metricsLookup)
		sort.Strings(cond.metricsRequested)
		sort.Strings(cond.metricsUnreverse)
		cond.prepareLookup()
		aggregations := map[string][]string{
			"avg": {"10_min.name.any", "1_min.name.avg"},
			"max": {"5_sec.name.max"},
			"min": {"5_min.name.min"},
		}
		assert.Equal(t, aggregations, cond.aggregations)
		// Steps saves only values, not the metrics list
		steps := map[uint32][]string{
			30:   {},
			60:   {},
			300:  {},
			1200: {},
		}
		assert.Equal(t, steps, cond.steps)
		bodies := make(map[string]string)
		for a, m := range aggregations {
			bodies[a] = strings.Join(m, "\n") + "\n"
		}
		assert.Equal(t, bodies, extTableString(cond.extDataBodies))

		cond.From = ageToTimestamp(1800)
		cond.Until = ageToTimestamp(0)
		cond.prepareLookup()
		steps = map[uint32][]string{
			30:  {},
			60:  {},
			300: {},
			5:   {},
		}
		assert.Equal(t, steps, cond.steps)
		assert.Equal(t, aggregations, cond.aggregations)
		assert.Equal(t, bodies, extTableString(cond.extDataBodies))
	})

	t.Run("non-aggregated non-reverse query", func(t *testing.T) {
		cond := newCondition(5400, 1800, 5)
		cond.aggregated = false
		cond.isReverse = false
		cond.prepareMetricsLists()
		sort.Strings(cond.metricsLookup)
		sort.Strings(cond.metricsRequested)
		sort.Strings(cond.metricsUnreverse)
		cond.prepareLookup()
		aggregations := map[string][]string{
			"avg": {"10_min.name.any", "1_min.name.avg"},
			"max": {"5_sec.name.max"},
			"min": {"5_min.name.min"},
		}
		assert.Equal(t, aggregations, cond.aggregations)
		// Steps saves only values, not the metrics list
		steps := map[uint32][]string{
			30:   {"10_min.name.any"},
			60:   {"5_sec.name.max"},
			300:  {"1_min.name.avg"},
			1200: {"5_min.name.min"},
		}
		assert.Equal(t, steps, cond.steps)
		bodies := map[string]string{"": "10_min.name.any\n1_min.name.avg\n5_min.name.min\n5_sec.name.max\n"}
		assert.Equal(t, bodies, extTableString(cond.extDataBodies))

		cond.From = ageToTimestamp(1800)
		cond.Until = ageToTimestamp(0)
		cond.prepareLookup()
		steps = map[uint32][]string{
			5:   {"5_sec.name.max"},
			30:  {"10_min.name.any"},
			60:  {"1_min.name.avg"},
			300: {"5_min.name.min"},
		}
		assert.Equal(t, steps, cond.steps)
		assert.Equal(t, aggregations, cond.aggregations)
		assert.Equal(t, bodies, extTableString(cond.extDataBodies))
	})

	t.Run("reverse query with improper rules", func(t *testing.T) {
		cond := newCondition(5400, 1800, 5)
		cond.aggregated = false
		cond.isReverse = true
		cond.prepareMetricsLists()
		sort.Strings(cond.metricsUnreverse)
		sort.Strings(cond.metricsRequested)
		cond.prepareLookup()
		aggregations := map[string][]string{
			"avg": {"10_min.name.any", "1_min.name.avg", "5_min.name.min", "5_sec.name.max"},
		}
		assert.Equal(t, aggregations, cond.aggregations)
		// Steps saves only values, not the metrics list
		steps := map[uint32][]string{
			30: {"10_min.name.any", "1_min.name.avg", "5_min.name.min", "5_sec.name.max"},
		}
		assert.Equal(t, steps, cond.steps)
		bodies := map[string]string{"": "any.name.10_min\navg.name.1_min\nmax.name.5_sec\nmin.name.5_min\n"}
		assert.Equal(t, bodies, extTableString(cond.extDataBodies))

		cond.From = ageToTimestamp(1800)
		cond.Until = ageToTimestamp(0)
		cond.prepareLookup()
		assert.Equal(t, steps, cond.steps)
		assert.Equal(t, aggregations, cond.aggregations)
		assert.Equal(t, bodies, extTableString(cond.extDataBodies))
	})

	t.Run("reverse query with proper rules", func(t *testing.T) {
		cond := newCondition(5400, 1800, 5)
		cond.rollupRules = newRules(true)
		cond.aggregated = false
		cond.isReverse = true
		cond.prepareMetricsLists()
		cond.prepareLookup()
		for a := range cond.aggregations {
			sort.Strings(cond.aggregations[a])
		}
		aggregations := map[string][]string{
			"avg": {"10_min.name.any", "1_min.name.avg"},
			"max": {"5_sec.name.max"},
			"min": {"5_min.name.min"},
		}
		assert.Equal(t, aggregations, cond.aggregations)
		// Steps saves only values, not the metrics list
		steps := map[uint32][]string{
			30:   {"10_min.name.any"},
			60:   {"5_sec.name.max"},
			300:  {"1_min.name.avg"},
			1200: {"5_min.name.min"},
		}
		assert.Equal(t, steps, cond.steps)

		cond.From = ageToTimestamp(1800)
		cond.Until = ageToTimestamp(0)
		cond.prepareLookup()
		steps = map[uint32][]string{
			5:   {"5_sec.name.max"},
			30:  {"10_min.name.any"},
			60:  {"1_min.name.avg"},
			300: {"5_min.name.min"},
		}
		for a := range cond.aggregations {
			sort.Strings(cond.aggregations[a])
		}
		assert.Equal(t, steps, cond.steps)
		assert.Equal(t, aggregations, cond.aggregations)
	})
}

func TestSetStep(t *testing.T) {
	t.Run("unaggregated max", func(t *testing.T) {
		cond := newCondition(1800, 0, 1)
		cond.prepareMetricsLists()
		cond.prepareLookup()
		cond.setStep(nil)
		var step int64 = 300
		assert.Equal(t, step, cond.step)

		cond.From = ageToTimestamp(5400)
		cond.prepareLookup()
		cond.setStep(nil)
		step = 1200
		assert.Equal(t, step, cond.step)
	})

	t.Run("aggregated common step", func(t *testing.T) {
		cStep := &commonStep{
			result: 0,
			wg:     sync.WaitGroup{},
			lock:   sync.RWMutex{},
		}

		cond := newCondition(1800, 0, 2)
		cond.aggregated = true
		cond.prepareMetricsLists()
		cond.prepareLookup()

		cStep.addTargets(1)
		cond.setStep(cStep)
		var step int64 = 1800 / 2
		assert.Equal(t, step, cond.step)

		cStep.addTargets(1)
		cStep.result = 0
		cond.From = ageToTimestamp(1200)
		cond.Until = ageToTimestamp(700)
		cond.MaxDataPoints = 5
		cond.setStep(cStep)
		step = 300
		assert.Equal(t, step, cond.step)

		cStep.addTargets(1)
		cStep.result = 0
		cond.MaxDataPoints = 10
		cond.steps = map[uint32][]string{1: {}, 5: {}, 3: {}, 4: {}}
		cond.setStep(cStep)
		step = 60
		assert.Equal(t, step, cond.step)

		cStep.addTargets(1)
		cStep.result = 0
		cond.MaxDataPoints = 7
		cond.steps = map[uint32][]string{1: {}, 5: {}, 8: {}, 4: {}}
		cond.setStep(cStep)
		step = 80
		assert.Equal(t, step, cond.step)

		cStep.addTargets(1)
		cond.MaxDataPoints = 6
		cond.setStep(cStep)
		step = 120
		assert.Equal(t, step, cond.step)
	})
}

func TestSetFromUntil(t *testing.T) {
	type in struct {
		from  int64
		until int64
		step  int64
	}
	type out struct {
		from  int64
		until int64
	}
	tests := []struct {
		in  in
		out out
	}{
		{in: in{4, 9, 2}, out: out{4, 9}},
		{in: in{4, 19, 3}, out: out{6, 20}},
		{in: in{4, 29, 5}, out: out{5, 29}},
		{in: in{7, 108, 7}, out: out{7, 111}},
		{in: in{7, 108, 13}, out: out{13, 116}},
	}

	for tn, test := range tests {
		t.Run(fmt.Sprintf("setFromUntil %d", tn), func(t *testing.T) {
			cond := &conditions{
				TimeFrame: &TimeFrame{From: test.in.from, Until: test.in.until},
				step:      test.in.step,
			}
			cond.setFromUntil()
			result := out{cond.from, cond.until}
			assert.Equal(t, test.out, result)
		})
	}
}

// prewhere, where and both generators are checked here
func TestGenerateQuery(t *testing.T) {
	table := "graphite.table"
	type in struct {
		from  int64
		until int64
		step  int64
		agg   string
	}
	tests := []struct {
		in           in
		aggregated   string
		unaggregated string
	}{
		{
			in: in{1668124800, 1668325322, 1, "avg"},
			aggregated: ("WITH anyResample(1668124800, 1668325322, 1)(toUInt32(intDiv(Time, 1)*1), Time) AS mask\n" +
				"SELECT Path,\n arrayFilter(m->m!=0, mask) AS times,\n" +
				" arrayFilter((v,m)->m!=0, avgResample(1668124800, 1668325322, 1)(Value, Time), mask) AS values\n" +
				"FROM graphite.table\n" +
				"PREWHERE Date >= '" + date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668325322) + "'\n" +
				"WHERE (Path in metrics_list) AND (Time >= 1668124800 AND Time <= 1668325322)\n" +
				"GROUP BY Path\n" +
				"FORMAT RowBinary"),
			unaggregated: ("SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp)\n" +
				"FROM graphite.table\n" +
				"PREWHERE Date >= '" + date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668325322) + "'\n" +
				"WHERE (Path in metrics_list) AND (Time >= 1668124800 AND Time <= 1668325322)\n" +
				"GROUP BY Path\n" +
				"FORMAT RowBinary"),
		},
		{
			in: in{11111, 33333, 11111, "min"},
			aggregated: ("WITH anyResample(11111, 33333, 11111)(toUInt32(intDiv(Time, 11111)*11111), Time) AS mask\n" +
				"SELECT Path,\n arrayFilter(m->m!=0, mask) AS times,\n" +
				" arrayFilter((v,m)->m!=0, minResample(11111, 33333, 11111)(Value, Time), mask) AS values\n" +
				"FROM graphite.table\n" +
				"PREWHERE Date >= '" + date.FromTimestampToDaysFormat(11111) + "' AND Date <= '" + date.FromTimestampToDaysFormat(33333) + "'\n" +
				"WHERE (Path in metrics_list) AND (Time >= 11111 AND Time <= 33333)\n" +
				"GROUP BY Path\n" +
				"FORMAT RowBinary"),
			unaggregated: ("SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp)\n" +
				"FROM graphite.table\n" +
				"PREWHERE Date >= '" + date.FromTimestampToDaysFormat(11111) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(33333) + "'\n" +
				"WHERE (Path in metrics_list) AND (Time >= 11111 AND Time <= 33333)\n" +
				"GROUP BY Path\n" +
				"FORMAT RowBinary"),
		},
	}
	for tn, test := range tests {
		t.Run(fmt.Sprintf("generate query %d", tn), func(t *testing.T) {
			cond := &conditions{
				Targets: &Targets{},
				from:    test.in.from,
				until:   test.in.until,
				step:    test.in.step,
			}
			cond.pointsTable = table
			cond.setPrewhere()
			cond.setWhere()
			unaggQuery := cond.generateQuery(test.in.agg)
			assert.Equal(t, test.unaggregated, unaggQuery)
			cond.aggregated = true
			aggQuery := cond.generateQuery(test.in.agg)
			assert.Equal(t, test.aggregated, aggQuery)
		})
	}
}
