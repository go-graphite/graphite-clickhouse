package reply

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/client"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/render/data"
)

var results = []client.Metric{
	{
		Name:           "test.metric1",
		PathExpression: "test.*",
		StartTime:      1688990040,
		StepTime:       60,
		StopTime:       1688990520,
		Values: func() []float64 {
			temp := emptyValues(8)
			temp[2] = 3
			return temp
		}(),
	},
	{
		Name:           "test.metric2",
		PathExpression: "test.*",
		StartTime:      1688990040,
		StepTime:       60,
		StopTime:       1688990520,
		Values:         emptyValues(8),
	},
	{
		Name:           "test.metric3",
		PathExpression: "test.*",
		StartTime:      1688990040,
		StepTime:       60,
		StopTime:       1688990520,
		Values:         emptyValues(8),
	},
}

func TestFormatterReply(t *testing.T) {
	formatters := []struct {
		impl   Formatter
		name   string
		format client.FormatType
	}{
		{&V3PB{}, "v3pb", client.FormatPb_v3},
		{&V2PB{}, "v2pb", client.FormatPb_v2},
		{&JSON{}, "json", client.FormatJSON},
		{&Pickle{}, "pickle", client.FormatPickle},
	}
	tests := []struct {
		name  string
		input data.CHResponses
		// result when CHResponse.AppendOutEmptySeries is false
		expectedWithoutEmpty []client.Metric
		// result when CHResponse.AppendOutEmptySeries is true
		expectedWithEmpty []client.Metric
	}{
		{
			name:                 "no index found",
			input:                data.EmptyResponse(),
			expectedWithoutEmpty: []client.Metric{},
			expectedWithEmpty:    []client.Metric{},
		},
		{
			name: "three metrics; test.metric1 with points and other with NaN",
			input: prepareCHResponses(1688990000, 1688990460,
				[][]byte{[]byte("test.metric1"), []byte("test.metric2"), []byte("test.metric3")},
				map[string][]point.Point{
					"test.metric1": {{Value: 3, Time: 1688990160, Timestamp: 1688990204}},
				},
			),
			expectedWithoutEmpty: results[:1],
			expectedWithEmpty:    results,
		},
		{
			name: "three metrics, no points in all",
			input: prepareCHResponses(1688990000, 1688990460,
				[][]byte{[]byte("test.metric1"), []byte("test.metric2"), []byte("test.metric3")},
				map[string][]point.Point{},
			),
			expectedWithoutEmpty: []client.Metric{},
			expectedWithEmpty: append([]client.Metric{
				{
					Name:           results[0].Name,
					PathExpression: results[0].PathExpression,
					StartTime:      results[0].StartTime,
					StopTime:       results[0].StopTime,
					StepTime:       results[0].StepTime,
					Values:         emptyValues(8),
				},
			}, results[1:]...),
		},
	}
	for _, formatter := range formatters {
		t.Run(fmt.Sprintf("format=%s", formatter.name), func(t *testing.T) {
			for _, tt := range tests {
				// case 0: test for AppendOutEmptySeries = false
				// case 1: test for AppendOutEmptySeries = true
				for i := 0; i < 2; i++ {
					var expected []client.Metric
					var testName string
					switch i {
					case 0:
						expected = tt.expectedWithoutEmpty
						testName = fmt.Sprintf("NoAppend: %s", tt.name)
						for j := range tt.input {
							tt.input[j].AppendOutEmptySeries = false
						}
					case 1:
						expected = tt.expectedWithEmpty
						testName = fmt.Sprintf("WithAppend: %s", tt.name)
						for j := range tt.input {
							tt.input[j].AppendOutEmptySeries = true
						}
					}

					t.Run(testName, func(t *testing.T) {
						ctx := context.Background()
						// if tt.protobufDebug {
						// 	ctx = scope.WithDebug(ctx, "Protobuf")
						// }
						w := httptest.NewRecorder()
						r, err := http.NewRequestWithContext(ctx, "", "", nil)
						if err != nil {
							require.NoErrorf(t, err, "failed to create request")
						}

						formatter.impl.Reply(w, r, tt.input)
						response := w.Result()
						defer response.Body.Close()

						// then
						require.Equal(t, http.StatusOK, response.StatusCode)
						data, err := io.ReadAll(response.Body)
						require.NoError(t, err)
						got, err := client.Decode(data, formatter.format)
						require.NoError(t, err)
						if !equalMetrics(expected, got) {
							t.Errorf("metrics not equal: expected:\n%#v\ngot:\n%#v\n", expected, got)
						}
					})
				}
			}
		})
	}
}

// prepareCHResponses prepares CHResponses for tests.
func prepareCHResponses(from, until int64, indices [][]byte, points map[string][]point.Point) data.CHResponses {
	// alias
	idx := finder.NewMockFinder(indices)
	m := alias.New()
	m.MergeTarget(idx, "test.*", false)

	// points
	pts := point.NewPoints()
	stringIndex := make([]string, 0, len(indices))
	for _, each := range indices {
		stringIndex = append(stringIndex, string(each))
	}
	for k, v := range points {
		id := pts.MetricID(k)
		for _, eachPoint := range v {
			pts.AppendPoint(id, eachPoint.Value, eachPoint.Time, eachPoint.Timestamp)
		}
	}
	pts.SetAggregations(map[string][]string{
		"avg": stringIndex,
	})
	sort.Sort(pts)
	return data.CHResponses{{
		Data: &data.Data{
			Points:     pts,
			AM:         m,
			CommonStep: 60,
		},
		From:  from,
		Until: until,
	}}
}

// emptyValues prefill slice of `size` with math.NaN
func emptyValues(size int) []float64 {
	arr := make([]float64, 0, size)
	for i := 0; i < size; i++ {
		arr = append(arr, math.NaN())
	}
	return arr
}

// equalMetrics returns true if two slices of client.Metric are equal.
// This function only compares important fields of client.Metric.
func equalMetrics(m1, m2 []client.Metric) bool {
	if len(m1) != len(m2) {
		return false
	}
	sort.Slice(m1, func(i, j int) bool {
		return m1[i].Name < m1[j].Name
	})
	sort.Slice(m2, func(i, j int) bool {
		return m2[i].Name < m2[j].Name
	})
	for i := 0; i < len(m1); i++ {
		// compare props
		if m1[i].Name != m2[i].Name ||
			m1[i].StartTime != m2[i].StartTime ||
			m1[i].StopTime != m2[i].StopTime ||
			m1[i].StepTime != m2[i].StepTime {
			return false
		}
		// compare values
		if len(m1[i].Values) != len(m2[i].Values) {
			return false
		}
		for j := 0; j < len(m1[i].Values); j++ {
			a, b := m1[i].Values[j], m2[i].Values[j]
			if math.IsNaN(a) && math.IsNaN(b) {
				continue
			}
			if a != b {
				return false
			}
		}
	}
	return true
}
