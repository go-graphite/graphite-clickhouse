package backend

import "testing"

func TestRollupParseXML(t *testing.T) {
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

	r, err := ParseRollupXML([]byte(config))
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

func PointsEq(t *testing.T, expected, actual []Point) {
	if len(actual) != len(expected) {
		t.Fatalf("len(actual) != len(expected): %d != %d", len(actual), len(expected))
	}

	for i := 0; i < len(actual); i++ {
		if (actual[i].Metric != expected[i].Metric) ||
			(actual[i].Time != expected[i].Time) ||
			(actual[i].Timestamp != expected[i].Timestamp) ||
			(actual[i].Value != expected[i].Value) {
			t.FailNow()
		}
	}
}

func TestPointsUniq(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 1, Value: 1},
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
	}

	for _, test := range tests {
		result := PointsUniq(test[0])
		PointsEq(t, test[1], result)
	}
}

func TestPointsCleanup(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 1, Value: 1},
				Point{Metric: "", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025152, Timestamp: 1, Value: 1},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{Metric: "", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{Metric: "", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
			},
		},
	}

	for _, test := range tests {
		result := PointsCleanup(test[0])
		PointsEq(t, test[1], result)
	}
}

func TestMetricPrecision(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Value: 3},
				Point{Metric: "metric", Time: 1478025154, Value: 2},
				Point{Metric: "metric", Time: 1478025255, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025120, Value: 5},
				Point{Metric: "metric", Time: 1478025240, Value: 1},
			},
		},
	}

	for _, test := range tests {
		result := doMetricPrecision(test[0], 60, aggrSum)
		PointsEq(t, test[1], result)
	}
}
