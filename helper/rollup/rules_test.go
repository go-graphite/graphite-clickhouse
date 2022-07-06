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
		assert.Equal(t, test[1], result)
	}
}

func TestLookup(t *testing.T) {
	config := `
	^hourly;;3600:60,86400:3600
	^live;;0:1
	total$;sum;
	<!PLAIN>sum$;sum;
	<!TAG_R>sum\?;sum;
	<!TAG_R>sum\?(.*&)*sampling=hourly(&|$);sum;3600:60,86400:600
	<!PLAIN>min$;min;
	<!TAG_R>min\?;min;
	<!PLAIN>max$;max;
	<!TAG_R>max\?;max;
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
		{"min?env=stage", "86401", "min", "10"},                  // tagged
		{"sum?env=stage&sampling=hourly", "86401", "sum", "600"}, // tagged
	}

	r, err := parseCompact(config, false)
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

var benchConfig = `
^hourly;;3600:60,86400:3600
^live;;0:1
\.fake1\..*\.Fake1\.;;3600:60,86400:3600
fake1\?(.*&)*tag=Fake1(&|$);;3600:60,86400:3600
\.fake3\..*\.Fake3\.;;3600:60,86400:3600
fake2\?(.*&)*tag=Fake2(&|$);;3600:60,86400:3600
\.fake3\..*\.Fake3\.;;3600:60,86400:3600
fake3\?(.*&)*tag=Fake3(&|$);;3600:60,86400:3600
\.fake4\..*\.Fake4\.;;3600:60,86400:3600
fake4\?(.*&)*tag=Fake4(&|$);;3600:60,86400:3600
total$;sum;
sum$;sum;
sum\?(.*&)*sampling=hourly(&|$);sum;3600:60,86400:3600
sum\?;sum;
min$;min;
min\?;min;
max$;max;
max\?;max;
;avg;
;;60:10
;;0:42`

// BenchmarkSumSeparated-6       Rules with type (all/plain/tagged_regex)
var benchConfigSeparated = `
<!PLAIN>^hourly;;3600:60,86400:3600
<!PLAIN>^live;;0:1
<!PLAIN>\.fake1\..*\.Fake1\.;;3600:60,86400:3600
<!TAG_R>fake1\?(.*&)*tag=Fake1(&|$);;3600:60,86400:3600
<!PLAIN>\.fake3\..*\.Fake3\.;;3600:60,86400:3600
<!TAG_R>fake2\?(.*&)*tag=Fake2(&|$);;3600:60,86400:3600
<!PLAIN>\.fake3\..*\.Fake3\.;;3600:60,86400:3600
<!TAG_R>fake3\?(.*&)*tag=Fake3(&|$);;3600:60,86400:3600
<!PLAIN>\.fake4\..*\.Fake4\.;;3600:60,86400:3600
<!TAG_R>fake4\?(.*&)*tag=Fake4(&|$);;3600:60,86400:3600
<!PLAIN>total$;sum;
<!PLAIN>sum$;sum;
<!TAG_R>sum\?(.*&)*sampling=hourly(&|$);sum;3600:60,86400:3600
<!TAG_R>sum\?;sum;
<!PLAIN>min$;min;
<!TAG_R>min\?;min;
<!PLAIN>max$;max;
<!TAG_R>max\?;max;
;avg;
;;60:10
;;0:42`

func BenchmarkSum(b *testing.B) {
	r, err := parseCompact(benchConfig, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.sum", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkSumAuto(b *testing.B) {
	r, err := parseCompact(benchConfig, true)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.sum", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkSumSeparated(b *testing.B) {
	r, err := parseCompact(benchConfigSeparated, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.sum", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkSumTagged(b *testing.B) {
	r, err := parseCompact(benchConfig, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("sum?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkSumTaggedAuto(b *testing.B) {
	r, err := parseCompact(benchConfig, true)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("sum?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkSumTaggedSeparated(b *testing.B) {
	r, err := parseCompact(benchConfigSeparated, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("sum?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkMax(b *testing.B) {
	r, err := parseCompact(benchConfig, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.max", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkMaxAuto(b *testing.B) {
	r, err := parseCompact(benchConfig, true)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.max", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkMaxSeparated(b *testing.B) {
	r, err := parseCompact(benchConfigSeparated, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.max", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkMaxTagged(b *testing.B) {
	r, err := parseCompact(benchConfig, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("max?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkMaxTaggedAuto(b *testing.B) {
	r, err := parseCompact(benchConfig, true)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("max?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkMaxTaggedSeparated(b *testing.B) {
	r, err := parseCompact(benchConfigSeparated, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("max?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkDefault(b *testing.B) {
	r, err := parseCompact(benchConfigSeparated, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.p95", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkDefaultAuto(b *testing.B) {
	r, err := parseCompact(benchConfig, true)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.p95", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkDefaultSeparated(b *testing.B) {
	r, err := parseCompact(benchConfigSeparated, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("test.p95", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkDefaultTagged(b *testing.B) {
	r, err := parseCompact(benchConfig, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("p95?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkDefaultTaggedAuto(b *testing.B) {
	r, err := parseCompact(benchConfig, true)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("p95?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}

func BenchmarkDefaultTaggedSeparated(b *testing.B) {
	r, err := parseCompact(benchConfigSeparated, false)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		precision, ag := r.Lookup("p95?env=test&tag=Fake5", 1)
		_ = precision
		_ = ag
	}
}
