package rollup

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/stretchr/testify/assert"
)

func TestMetricPrecision(t *testing.T) {
	tests := [][2][]point.Point{
		{
			{ // in
				{MetricID: 1, Time: 1478025152, Value: 3},
				{MetricID: 1, Time: 1478025154, Value: 2},
				{MetricID: 1, Time: 1478025255, Value: 1},
			},
			{ // out
				{MetricID: 1, Time: 1478025120, Value: 5},
				{MetricID: 1, Time: 1478025240, Value: 1},
			},
		},
	}

	for _, test := range tests {
		result := doMetricPrecision(test[0], 60, AggrMap["sum"])
		point.AssertListEq(t, test[1], result)
	}
}

func TestLookup(t *testing.T) {
	config := `
	^hourly;;3600:60,86400:3600
	^live;;0:1
	total$;sum;
	min$;min;
	max$;max;
	;avg;
	;;60:10
	;;0:42`

	table := [][4]string{
		{"hello.world", "0", "avg", "42"},
		{"hourly.rps", "0", "avg", "42"},
		{"hourly.rps_total", "0", "sum", "42"},
		{"live.rps_total", "0", "sum", "1"},
		{"hourly.rps_min", "0", "min", "42"},
		{"hourly.rps_min", "1", "min", "42"},
		{"hourly.rps_min", "59", "min", "42"},
		{"hourly.rps_min", "60", "min", "10"},
		{"hourly.rps_min", "61", "min", "10"},
		{"hourly.rps_min", "3599", "min", "10"},
		{"hourly.rps_min", "3600", "min", "60"},
		{"hourly.rps_min", "3601", "min", "60"},
		{"hourly.rps_min", "86399", "min", "60"},
		{"hourly.rps_min", "86400", "min", "3600"},
		{"hourly.rps_min", "86401", "min", "3600"},
	}

	r, err := parseCompact(config)
	assert.NoError(t, err)

	for _, c := range table {
		t.Run(fmt.Sprintf("%#v", c[:]), func(t *testing.T) {
			assert := assert.New(t)
			age, err := strconv.Atoi(c[1])
			assert.NoError(err)

			precision, ag := r.Lookup(c[0], uint32(age))
			assert.Equal(c[2], ag.String())
			assert.Equal(c[3], fmt.Sprintf("%d", precision))
		})
	}
}
