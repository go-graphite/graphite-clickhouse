package reply

import (
	"bufio"
	"bytes"
	"math"
	"reflect"
	"testing"

	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/helper/point"
)

type testV3PB struct {
	name     string
	target   string
	function string
	response v3pb.MultiFetchResponse
	from     uint32
	until    uint32
	step     uint32
	points   []point.Point
}

func TestV3PBWriteBody(t *testing.T) {
	tests := []testV3PB{
		{
			name:     "singlePoint",
			function: "avg",
			from:     4,
			until:    13,
			step:     5,
			target:   "*",
			points: []point.Point{
				{
					MetricID:  0,
					Value:     1.0,
					Time:      5,
					Timestamp: 5,
				},
			},
			response: v3pb.MultiFetchResponse{
				Metrics: []v3pb.FetchResponse{
					{
						Name:                    "singlePoint",
						PathExpression:          "*",
						ConsolidationFunc:       "avg",
						XFilesFactor:            0,
						HighPrecisionTimestamps: false,
						StartTime:               5,
						StopTime:                10,
						Values:                  []float64{1.0},
						AppliedFunctions:        []string{},
						RequestStartTime:        4,
						RequestStopTime:         13,
					},
				},
			},
		},
		{
			name:     "multiPoint",
			function: "max",
			from:     1,
			until:    5,
			step:     1,
			target:   "multiPoint",
			points: []point.Point{
				{
					MetricID:  0,
					Value:     1.0,
					Time:      2,
					Timestamp: 2,
				},
				{
					MetricID:  0,
					Value:     math.NaN(),
					Time:      3,
					Timestamp: 3,
				},
				{
					MetricID:  0,
					Value:     3.0,
					Time:      4,
					Timestamp: 4,
				},
			},
			response: v3pb.MultiFetchResponse{
				Metrics: []v3pb.FetchResponse{
					{
						Name:                    "multiPoint",
						PathExpression:          "multiPoint",
						ConsolidationFunc:       "max",
						XFilesFactor:            0,
						HighPrecisionTimestamps: false,
						StartTime:               1,
						StopTime:                6,
						Values:                  []float64{math.NaN(), 1.0, math.NaN(), 3.0, math.NaN()},
						AppliedFunctions:        []string{},
						RequestStartTime:        1,
						RequestStopTime:         6,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		testName := tt.name

		t.Run(testName, func(t *testing.T) {
			correctResp, _ := tt.response.Marshal()

			b := bytes.Buffer{}
			w := bufio.NewWriter(&b)

			v := &V3PB{}
			v.initBuffer()
			v.writeBody(w, tt.target, tt.name, tt.function, tt.from, tt.until, tt.step, tt.points)

			w.Flush()

			var resp v3pb.MultiFetchResponse

			data := b.Bytes()
			if bytes.Compare(data, correctResp) != 0 {
				t.Logf("different byte response.\ngot:\n%v\n\nexpected:\n%v", data, correctResp)
			}

			err := resp.Unmarshal(data)
			if err != nil {
				t.Fatalf("failed to unmarshal reply, got '%v'", err)
			}

			if len(resp.Metrics) != len(tt.response.Metrics) {
				t.Fatalf("incorrect amount of metrics, expected %v, got %v", len(resp.Metrics), len(tt.response.Metrics))
			}

			for i := range resp.Metrics {
				if resp.Metrics[i].Name != tt.response.Metrics[i].Name {
					if !reflect.DeepEqual(resp.Metrics[i], tt.response.Metrics[i]) {
						t.Fatalf("replies are not same.\ngot:\n%+v\n\nexpected:\n%+v", resp.Metrics[i], tt.response.Metrics[i])
					}
				}
			}
		})
	}
}
