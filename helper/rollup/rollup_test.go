package rollup

import (
	"fmt"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/point"
)

func TestParseXML(t *testing.T) {
	config := `
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
`

	r, err := ParseXML([]byte(config))
	if err != nil {
		t.Fatal(err)
	}

	if r.Pattern[0].Retention[1].Age != 86400 {
		t.FailNow()
	}

	if r.Default.Retention[2].Precision != 3600 {
		t.FailNow()
	}
}

func TestMetricPrecision(t *testing.T) {
	tests := [][2][]point.Point{
		{
			{ // in
				{Metric: "metric", Time: 1478025152, Value: 3},
				{Metric: "metric", Time: 1478025154, Value: 2},
				{Metric: "metric", Time: 1478025255, Value: 1},
			},
			{ // out
				{Metric: "metric", Time: 1478025120, Value: 5},
				{Metric: "metric", Time: 1478025240, Value: 1},
			},
		},
	}

	for _, test := range tests {
		result := doMetricPrecision(test[0], 60, AggrSum)
		point.AssertListEq(t, test[1], result)
	}
}

func TestMetricStep(t *testing.T) {
	config := `
<graphite_rollup>
 	<pattern>
 		<regexp>^metric\.</regexp>
 		<function>any</function>
 		<retention>
 			<age>0</age>
 			<precision>1</precision>
 		</retention>
 		<retention>
 			<age>3600</age>
 			<precision>10</precision>
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
`
	r, err := ParseXML([]byte(config))
	if err != nil {
		t.Fatal(err)
	}
	now := int32(time.Now().Unix())

	tests := []struct {
		name         string
		from         int32
		expectedStep int32
	}{
		{
			name:         "metric.foo1",
			from:         now - 500,
			expectedStep: 1,
		},
		{
			name:         "metric.foo2",
			from:         now - 3600,
			expectedStep: 10,
		},
		{
			name:         "foo.bar1",
			from:         now - 500,
			expectedStep: 60,
		},
		{
			name:         "foo.bar2",
			from:         now - 3700,
			expectedStep: 300,
		},
		{
			name:         "foo.bar3",
			from:         now - 87000,
			expectedStep: 3600,
		},
	}

	for _, test := range tests {
		fmt.Printf("Performing test for metric=%v (from=now-%v)\n", test.name, now-test.from)
		step := r.Step(test.name, test.from)
		if step != test.expectedStep {
			t.Errorf("metric=%v (from=now-%v), expected step=%v, actual step=%v", test.name, now-test.from, test.expectedStep, step)
		}
	}
}
