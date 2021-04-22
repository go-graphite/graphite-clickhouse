package reply

import (
	"bufio"
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/lomik/graphite-clickhouse/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/helper/point"
)

type test struct {
	name     string
	target   string
	function string
	response carbonapi_v3_pb.MultiFetchResponse
	from     uint32
	until    uint32
	step     uint32
	points   []point.Point
}

func TestWritePB3(t *testing.T) {
	tests := []test{
		{
			name:     "singlePoint",
			function: "avg",
			from:     4,
			until:    13,
			step:     5,
			target:   "*",
			points: []point.Point{
				{
					0,
					1.0,
					5,
					5,
				},
			},
			response: carbonapi_v3_pb.MultiFetchResponse{
				Metrics: []carbonapi_v3_pb.FetchResponse{
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
					0,
					1.0,
					2,
					2,
				},
				{
					0,
					math.NaN(),
					3,
					3,
				},
				{
					0,
					3.0,
					4,
					4,
				},
			},
			response: carbonapi_v3_pb.MultiFetchResponse{
				Metrics: []carbonapi_v3_pb.FetchResponse{
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

			mb := new(bytes.Buffer)
			mb2 := new(bytes.Buffer)
			writePB3(mb, mb2, w, tt.target, tt.name, tt.function, tt.from, tt.until, tt.step, tt.points)

			w.Flush()

			var resp carbonapi_v3_pb.MultiFetchResponse

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
