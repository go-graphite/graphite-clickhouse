package rollup

import (
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func Test_buildTaggedRegex(t *testing.T) {
	tests := []struct {
		tagsStr string
		want    string
		match   string
		nomatch string
	}{
		{
			tagsStr: `cpu\.loadavg;project=DB.*;env=st.*`, want: `^cpu\.loadavg\?(.*&)?env=st.*&(.*&)?project=DB.*(&.*)?$`,
			match:   `cpu.loadavg?env=staging&project=DBAAS`,
			nomatch: `cpu.loadavg?env=staging&project=D`,
		},
		{
			tagsStr: `project=DB.*;env=staging;`, want: `[\?&]env=staging&(.*&)?project=DB.*(&.*)?$`,
			match:   `cpu.loadavg?env=staging&project=DBPG`,
			nomatch: `cpu.loadavg?env=stagingN&project=DBAAS`,
		},
		{
			tagsStr: "env=staging;", want: `[\?&]env=staging(&.*)?$`,
			match:   `cpu.loadavg?env=staging&project=DPG`,
			nomatch: `cpu.loadavg?env=stagingN`,
		},
		{
			tagsStr: " env = staging ;", // spaces are allowed,
			want:    `[\?&] env = staging (&.*)?$`,
			match:   `cpu.loadavg? env = staging &project=DPG`,
			nomatch: `cpu.loadavg?env=stagingN`,
		},
		{
			tagsStr: "name;",
			want:    `^name\?`,
			match:   `name?env=staging&project=DPG`,
			nomatch: `nameN?env=stagingN`,
		},
		{
			tagsStr: "name",
			want:    `^name\?`,
			match:   `name?env=staging&project=DPG`,
			nomatch: `nameN?env=stagingN`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.tagsStr, func(t *testing.T) {
			if got := buildTaggedRegex(tt.tagsStr); got != tt.want {
				t.Errorf("buildTaggedRegex(%q) = %v, want %v", tt.tagsStr, got, tt.want)
			} else {
				re := regexp.MustCompile(got)
				if tt.match != "" && !re.Match([]byte(tt.match)) {
					t.Errorf("match(%q, %q) must be true", tt.tagsStr, tt.match)
				}
				if tt.nomatch != "" && re.Match([]byte(tt.nomatch)) {
					t.Errorf("match(%q, %q) must be false", tt.tagsStr, tt.match)
				}
			}
		})
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
	require.NoError(t, err)

	for _, c := range table {
		t.Run(fmt.Sprintf("%#v", c[:]), func(t *testing.T) {
			assert := assert.New(t)
			age, err := strconv.Atoi(c[1])
			assert.NoError(err)

			precision, ag, _, _ := r.Lookup(c[0], uint32(age), false)
			assert.Equal(c[2], ag.String())
			assert.Equal(c[3], fmt.Sprintf("%d", precision))
		})
	}
}

func TestLookupTyped(t *testing.T) {
	config := `
	<graphite_rollup>
	 	<pattern>
	 		<regexp>^hourly</regexp>
	 		<retention>
	 			<age>3600</age>
	 			<precision>60</precision>
	 		</retention>
	 		<retention>
	 			<age>86400</age>
	 			<precision>3600</precision>
	 		</retention>
		</pattern>
		<pattern>
	 		<regexp>^live</regexp>
	 		<retention>
	 			<age>0</age>
	 			<precision>1</precision>
	 		</retention>
		</pattern>
		<pattern>
			<rule_type>tag_list</rule_type>
			<regexp>fake3;tag3=Fake3</regexp>
			<retention>
	 			<age>0</age>
	 			<precision>1</precision>
	 		</retention>
 		</pattern>
		 <pattern>
		 <rule_type>tag_list</rule_type>
		 <regexp>tag5=Fake5;tag3=Fake3</regexp>
		 <retention>
			  <age>0</age>
			  <precision>90</precision>
		  </retention>
	  </pattern>
		<pattern>
			<rule_type>tag_list</rule_type>
			<regexp>fake_name</regexp>
			<retention>
				<age>0</age>
				<precision>20</precision>
		  	</retention>
	  	</pattern>
		<pattern>
			<rule_type>plain</rule_type>
			<regexp>total$</regexp>
			<function>sum</function>
   		</pattern>
		<pattern>
		   <rule_type>plain</rule_type>
		   <regexp>min$</regexp>
		   <function>min</function>
		</pattern>
		<pattern>
		   <rule_type>plain</rule_type>
		   <regexp>max$</regexp>
		   <function>max</function>
		</pattern>
		<pattern>
			<rule_type>tagged</rule_type>
			<regexp>total?</regexp>
			<function>sum</function>
   		</pattern>
		<pattern>
		   <rule_type>tagged</rule_type>
		   <regexp>min\?</regexp>
		   <function>min</function>
		</pattern>
		<pattern>
		   <rule_type>tagged</rule_type>
		   <regexp>max\?</regexp>
		   <function>max</function>
		</pattern>
		<pattern>
			<rule_type>tagged</rule_type>
			<regexp>^hourly</regexp>
			<function>sum</function>
		</pattern>
	 	<default>
	 		<function>avg</function>
	 		<retention>
	 			<age>0</age>
	 			<precision>42</precision>
	 		</retention>
	 		<retention>
	 			<age>60</age>
	 			<precision>10</precision>
	 		</retention>
	 	</default>
	</graphite_rollup>
	`

	table := [][4]string{
		{"hello.world", "0", "avg", "42"},
		{"hourly.rps", "0", "avg", "42"},
		{"hourly.rps?tag=value", "0", "sum", "42"},
		{"hourly.rps", "0", "avg", "42"},
		{"hourly.rps_total", "0", "sum", "42"},
		{"live.rps_total", "0", "sum", "1"},
		{"hourly.rps_min", "0", "min", "42"},
		{"hourly.rps_min?tag=value", "0", "min", "42"},
		{"hourly.rps_min", "1", "min", "42"},
		{"hourly.rps_min", "59", "min", "42"},
		{"hourly.rps_min?tag=value", "59", "min", "42"},
		{"hourly.rps_min", "60", "min", "10"},
		{"hourly.rps_min", "61", "min", "10"},
		{"hourly.rps_min", "3599", "min", "10"},
		{"hourly.rps_min", "3600", "min", "60"},
		{"hourly.rps_min", "3601", "min", "60"},
		{"hourly.rps_min", "86399", "min", "60"},
		{"hourly.rps_min", "86400", "min", "3600"},
		{"hourly.rps_min", "86401", "min", "3600"},
		{"fake3?tag3=Fake3", "0", "avg", "1"},
		{"fake3?tag1=Fake1&tag3=Fake3", "0", "avg", "1"},
		{"fake3?tag1=Fake1&tag3=Fake3&tag4=Fake4", "0", "avg", "1"},
		{"fake3?tag3=Fake", "0", "avg", "42"},
		{"fake3?tag1=Fake1&tag3=Fake", "0", "avg", "42"},
		{"fake3?tag1=Fake1&tag3=Fake&tag4=Fake4", "0", "avg", "42"},
		{"fake?tag3=Fake3", "0", "avg", "42"},
		{"fake_name?tag3=Fake3", "0", "avg", "20"},
		{"fake5?tag1=Fake1&tag3=Fake3&tag4=Fake4&tag5=Fake5", "0", "avg", "90"},
		{"fake5?tag3=Fake3&tag4=Fake4&tag5=Fake5&tag6=Fake6", "0", "avg", "90"},
		{"fake5?tag4=Fake4&tag5=Fake5&tag6=Fake6", "0", "avg", "42"},
	}

	r, err := parseXML([]byte(config))
	require.NoError(t, err)

	for _, c := range table {
		t.Run(fmt.Sprintf("%#v", c[:]), func(t *testing.T) {
			assert := assert.New(t)
			age, err := strconv.Atoi(c[1])
			assert.NoError(err)

			precision, ag, _, _ := r.Lookup(c[0], uint32(age), false)
			assert.Equal(c[2], ag.String())
			assert.Equal(c[3], fmt.Sprintf("%d", precision))
		})
	}
}

func TestRules_RollupPoints(t *testing.T) {
	config := `
	^10sec;;0:10,3600:60
	;max;0:20`

	r, err := parseCompact(config)
	require.NoError(t, err)

	timeNow = func() time.Time {
		return time.Unix(10010, 0)
	}

	newPoints := func() *point.Points {
		pp := point.NewPoints()

		id10Sec := pp.MetricID("10sec")
		pp.AppendPoint(id10Sec, 1.0, 10, 0)
		pp.AppendPoint(id10Sec, 2.0, 20, 0)
		pp.AppendPoint(id10Sec, 3.0, 30, 0)
		pp.AppendPoint(id10Sec, 6.0, 60, 0)
		pp.AppendPoint(id10Sec, 7.0, 70, 0)

		idDefault := pp.MetricID("default")
		pp.AppendPoint(idDefault, 2.0, 20, 0)
		pp.AppendPoint(idDefault, 4.0, 40, 0)
		pp.AppendPoint(idDefault, 6.0, 60, 0)
		pp.AppendPoint(idDefault, 8.0, 80, 0)

		return pp
	}

	pointsTo60SecNoDefault := func() *point.Points {
		pp := point.NewPoints()

		id10Sec := pp.MetricID("10sec")
		pp.AppendPoint(id10Sec, 3.0, 0, 0)
		pp.AppendPoint(id10Sec, 7.0, 60, 0)

		idDefault := pp.MetricID("default")
		pp.AppendPoint(idDefault, 2.0, 20, 0)
		pp.AppendPoint(idDefault, 4.0, 40, 0)
		pp.AppendPoint(idDefault, 6.0, 60, 0)
		pp.AppendPoint(idDefault, 8.0, 80, 0)

		return pp
	}

	pointsTo60Sec := func() *point.Points {
		pp := point.NewPoints()

		id10Sec := pp.MetricID("10sec")
		pp.AppendPoint(id10Sec, 3.0, 0, 0)
		pp.AppendPoint(id10Sec, 7.0, 60, 0)

		idDefault := pp.MetricID("default")
		pp.AppendPoint(idDefault, 4.0, 0, 0)
		pp.AppendPoint(idDefault, 8.0, 60, 0)

		return pp
	}

	tests := []struct {
		name    string
		pp      *point.Points
		from    int64
		step    int64
		want    *point.Points
		wantErr bool
	}{
		{
			name: "without step and no rollup",
			pp:   newPoints(),
			from: int64(10000), step: int64(0),
			want: newPoints(),
		},
		{
			name: "without step",
			pp:   newPoints(),
			from: int64(10), step: int64(0),
			want: pointsTo60SecNoDefault(),
		},
		{
			name: "with step 10",
			pp:   newPoints(),
			from: int64(10000), step: int64(10),
			want: newPoints(),
		},
		{
			name: "with step 60",
			pp:   newPoints(),
			from: int64(10), step: int64(60),
			want: pointsTo60Sec(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := r.RollupPoints(tt.pp, tt.from, tt.step); (err != nil) != tt.wantErr {
				t.Errorf("Rules.RollupPoints() error = %v, wantErr %v", err, tt.wantErr)
			} else if err == nil {
				assert.Equal(t, tt.want, tt.pp)
			}
		})
	}
}

var benchConfig = `
	<graphite_rollup>
	 	<pattern>
	 		<regexp>^hourly</regexp>
	 		<retention>
	 			<age>3600</age>
	 			<precision>60</precision>
	 		</retention>
	 		<retention>
	 			<age>86400</age>
	 			<precision>3600</precision>
	 		</retention>
		</pattern>
		<pattern>
	 		<regexp>^live</regexp>
	 		<retention>
	 			<age>0</age>
	 			<precision>1</precision>
	 		</retention>
		</pattern>
		<pattern>
			<regexp>\.fake1\..*\.Fake1\.</regexp>
			<retention>
	 			<age>3600</age>
	 			<precision>60</precision>
	 		</retention>
	 		<retention>
	 			<age>86400</age>
	 			<precision>3600</precision>
	 		</retention>
 		</pattern>
		<pattern>
			<regexp><![CDATA[fake1\?(.*&)*tag=Fake1(&|$)]]></regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	  	</pattern>
		<pattern>
			<regexp>\.fake2\..*\.Fake2\.</regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	   	</pattern>
	  	<pattern>
		  <regexp><![CDATA[fake2\?(.*&)*tag=Fake2(&|$)]]></regexp>
		  <retention>
			  <age>3600</age>
			  <precision>60</precision>
		  </retention>
		  <retention>
			  <age>86400</age>
			  <precision>3600</precision>
		  </retention>
		</pattern>
		<pattern>
			<regexp>\.fake3\..*\.Fake3\.</regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	   	</pattern>
	  	<pattern>
		  <regexp><![CDATA[fake3\?(.*&)*tag=Fake3(&|$)]]></regexp>
		  <retention>
			  <age>3600</age>
			  <precision>60</precision>
		  </retention>
		  <retention>
			  <age>86400</age>
			  <precision>3600</precision>
		  </retention>
		</pattern>
		<pattern>
			<regexp>\.fake4\..*\.Fake4\.</regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	   	</pattern>
	  	<pattern>
		  <regexp><![CDATA[fake\?(.*&)*tag=Fake4(&|$)]]></regexp>
		  <retention>
			  <age>3600</age>
			  <precision>60</precision>
		  </retention>
		  <retention>
			  <age>86400</age>
			  <precision>3600</precision>
		  </retention>
		</pattern>
		<pattern>
			<regexp>total$</regexp>
			<function>sum</function>
   		</pattern>
		<pattern>
		   <regexp>min$</regexp>
		   <function>min</function>
		</pattern>
		<pattern>
		   <regexp>max$</regexp>
		   <function>max</function>
		</pattern>
		<pattern>
			<regexp>total?</regexp>
			<function>sum</function>
   		</pattern>
		<pattern>
		   <regexp>min\?</regexp>
		   <function>min</function>
		</pattern>
		<pattern>
		   <regexp>max\?</regexp>
		   <function>max</function>
		</pattern>
		<pattern>
			<regexp>^hourly</regexp>
			<function>sum</function>
		</pattern>
	 	<default>
	 		<function>avg</function>
	 		<retention>
	 			<age>0</age>
	 			<precision>42</precision>
	 		</retention>
	 		<retention>
	 			<age>60</age>
	 			<precision>10</precision>
	 		</retention>
	 	</default>
	</graphite_rollup>
	`

var benchConfigSeparated = `
	<graphite_rollup>
	 	<pattern>
			<rule_type>plain</rule_type>
	 		<regexp>^hourly</regexp>
	 		<retention>
	 			<age>3600</age>
	 			<precision>60</precision>
	 		</retention>
	 		<retention>
	 			<age>86400</age>
	 			<precision>3600</precision>
	 		</retention>
		</pattern>
		<pattern>
			<rule_type>plain</rule_type>
	 		<regexp>^live</regexp>
	 		<retention>
	 			<age>0</age>
	 			<precision>1</precision>
	 		</retention>
		</pattern>
		<pattern>
			<rule_type>plain</rule_type>
			<regexp>\.fake1\..*\.Fake1\.</regexp>
			<retention>
	 			<age>3600</age>
	 			<precision>60</precision>
	 		</retention>
	 		<retention>
	 			<age>86400</age>
	 			<precision>3600</precision>
	 		</retention>
 		</pattern>
		<pattern>
			<rule_type>tagged</rule_type>
			<regexp><![CDATA[fake1\?(.*&)*tag=Fake1(&|$)]]></regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	  	</pattern>
		<pattern>
			<rule_type>plain</rule_type>
			<regexp>\.fake2\..*\.Fake2\.</regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	   	</pattern>
	  	<pattern>
		  <rule_type>tagged</rule_type>
		  <regexp><![CDATA[fake2\?(.*&)*tag=Fake2(&|$)]]></regexp>
		  <retention>
			  <age>3600</age>
			  <precision>60</precision>
		  </retention>
		  <retention>
			  <age>86400</age>
			  <precision>3600</precision>
		  </retention>
		</pattern>
		<pattern>
			<rule_type>plain</rule_type>
			<regexp>\.fake3\..*\.Fake3\.</regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	   	</pattern>
	  	<pattern>
		  <rule_type>tagged</rule_type>
		  <regexp><![CDATA[fake3\?(.*&)*tag=Fake3(&|$)]]></regexp>
		  <retention>
			  <age>3600</age>
			  <precision>60</precision>
		  </retention>
		  <retention>
			  <age>86400</age>
			  <precision>3600</precision>
		  </retention>
		</pattern>
		<pattern>
			<rule_type>plain</rule_type>
			<regexp>\.fake4\..*\.Fake4\.</regexp>
			<retention>
				<age>3600</age>
				<precision>60</precision>
			</retention>
			<retention>
				<age>86400</age>
				<precision>3600</precision>
			</retention>
	   	</pattern>
	  	<pattern>
		  <rule_type>tagged</rule_type>
		  <regexp><![CDATA[fake\?(.*&)*tag=Fake4(&|$)]]></regexp>
		  <retention>
			  <age>3600</age>
			  <precision>60</precision>
		  </retention>
		  <retention>
			  <age>86400</age>
			  <precision>3600</precision>
		  </retention>
		</pattern>
		<pattern>
			<rule_type>plain</rule_type>
			<regexp>total$</regexp>
			<function>sum</function>
   		</pattern>
		<pattern>
		   <rule_type>plain</rule_type>
		   <regexp>min$</regexp>
		   <function>min</function>
		</pattern>
		<pattern>
		   <rule_type>plain</rule_type>
		   <regexp>max$</regexp>
		   <function>max</function>
		</pattern>
		<pattern>
			<rule_type>tagged</rule_type>
			<regexp>total?</regexp>
			<function>sum</function>
   		</pattern>
		<pattern>
		   <rule_type>tagged</rule_type>
		   <regexp>min\?</regexp>
		   <function>min</function>
		</pattern>
		<pattern>
		   <rule_type>tagged</rule_type>
		   <regexp>max\?</regexp>
		   <function>max</function>
		</pattern>
		<pattern>
			<rule_type>tagged</rule_type>
			<regexp>^hourly</regexp>
			<function>sum</function>
		</pattern>
	 	<default>
	 		<function>avg</function>
	 		<retention>
	 			<age>0</age>
	 			<precision>42</precision>
	 		</retention>
	 		<retention>
	 			<age>60</age>
	 			<precision>10</precision>
	 		</retention>
	 	</default>
	</graphite_rollup>
	`

func BenchmarkLookupSum(b *testing.B) {
	r, err := parseXML([]byte(benchConfig))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("test.sum", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupSumSeparated(b *testing.B) {
	r, err := parseXML([]byte(benchConfigSeparated))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("test.sum", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupSumTagged(b *testing.B) {
	r, err := parseXML([]byte(benchConfig))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("sum?env=test&tag=Fake5", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupSumTaggedSeparated(b *testing.B) {
	r, err := parseXML([]byte(benchConfigSeparated))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("sum?env=test&tag=Fake5", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupMax(b *testing.B) {
	r, err := parseXML([]byte(benchConfig))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("test.max", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupMaxSeparated(b *testing.B) {
	r, err := parseXML([]byte(benchConfigSeparated))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("test.max", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupMaxTagged(b *testing.B) {
	r, err := parseXML([]byte(benchConfig))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("max?env=test&tag=Fake5", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupMaxTaggedSeparated(b *testing.B) {
	r, err := parseXML([]byte(benchConfigSeparated))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("max?env=test&tag=Fake5", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupDefault(b *testing.B) {
	r, err := parseXML([]byte(benchConfig))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("test.p95", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupDefaultSeparated(b *testing.B) {
	r, err := parseXML([]byte(benchConfigSeparated))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("test.p95", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupDefaultTagged(b *testing.B) {
	r, err := parseXML([]byte(benchConfig))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("p95?env=test&tag=Fake5", 1, false)
		_ = precision
		_ = ag
	}
}

func BenchmarkLookupDefaultTaggedSeparated(b *testing.B) {
	r, err := parseXML([]byte(benchConfigSeparated))
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		precision, ag, _, _ := r.Lookup("p95?env=test&tag=Fake5", 1, false)
		_ = precision
		_ = ag
	}
}
