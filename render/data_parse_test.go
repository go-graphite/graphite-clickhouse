package render

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/stretchr/testify/assert"
)

type pointValues struct {
	Values     []float64
	Times      []uint32
	Timestamps []uint32
}

type testPoint struct {
	Metric      string
	PointValues *pointValues
}

func makeData(points []testPoint) []byte {
	buf := new(bytes.Buffer)
	w := RowBinary.NewEncoder(buf)

	for i := 0; i < len(points); i++ {
		w.String(points[i].Metric)
		w.Uint32List(points[i].PointValues.Times)
		w.Float64List(points[i].PointValues.Values)
		w.Uint32List(points[i].PointValues.Timestamps)
	}

	return buf.Bytes()
}

func TestDataParse(t *testing.T) {
	t.Run("empty response", func(t *testing.T) {
		body := []byte{}
		r := bytes.NewReader(body)

		d, err := DataParse(r, nil, false)
		assert.NoError(t, err)
		assert.Empty(t, d.Points.List())
	})

	t.Run("ok", func(t *testing.T) {
		table := [][]testPoint{
			{
				{"hello.world", &pointValues{[]float64{42.1}, []uint32{1520056686}, []uint32{1520056706}}},
			},
			{
				{"hello.world", &pointValues{[]float64{42.1}, []uint32{1520056686}, []uint32{1520056706}}},
				{"foobar", &pointValues{[]float64{42.2}, []uint32{1520056687}, []uint32{1520056707}}},
			},
			{
				{"samelen1", &pointValues{[]float64{42.1}, []uint32{1520056686}, []uint32{1520056706}}},
				{"samelen2", &pointValues{[]float64{42.2}, []uint32{1520056687}, []uint32{1520056707}}},
			},
			{
				{"key1", &pointValues{[]float64{42.1, 42.2},
					[]uint32{1520056686, 1520056687}, []uint32{1520056706, 1520056687}}},
				{"key2", &pointValues{[]float64{42.2}, []uint32{1520056687}, []uint32{1520056707}}},
			},
		}

		for i := 0; i < len(table); i++ {
			t.Run(fmt.Sprintf("ok #%d", i), func(t *testing.T) {
				body := makeData(table[i])

				r := bytes.NewReader(body)

				d, err := DataParse(r, nil, false)
				// point number
				p := 0
				assert.NoError(t, err)
				for j := 0; j < len(table[i]); j++ {
					for m := 0; m < len(table[i][j].PointValues.Times); m++ {
						assert.Equal(t, table[i][j].Metric, d.Points.MetricName(d.Points.List()[p].MetricID))
						assert.Equal(t, table[i][j].PointValues.Times[m], d.Points.List()[p].Time)
						assert.Equal(t, table[i][j].PointValues.Values[m], d.Points.List()[p].Value)
						assert.Equal(t, table[i][j].PointValues.Timestamps[m], d.Points.List()[p].Timestamp)
						p++
					}
				}
			})
		}
	})

	t.Run("malformed ClickHouse body", func(t *testing.T) {
		body := makeData([]testPoint{
			{
				Metric: "hello.world",
				PointValues: &pointValues{
					Values:     []float64{42.1},
					Times:      []uint32{1520056686},
					Timestamps: []uint32{1520056706, 1520056707},
				},
			},
		})
		r := bytes.NewReader(body)

		_, err := DataParse(r, nil, false)
		assert.Error(t, err)
	})

	t.Run("incomplete response", func(t *testing.T) {
		body := makeData([]testPoint{
			{
				Metric: "hello.world",
				PointValues: &pointValues{
					Values:     []float64{42.1},
					Times:      []uint32{1520056686},
					Timestamps: []uint32{1520056706},
				},
			},
		})

		for i := 1; i < len(body)-1; i++ {
			r := bytes.NewReader(body[:i])

			d, err := DataParse(r, nil, false)
			fmt.Printf("%s %#v\n", err.Error(), d)
			assert.Error(t, err)
			assert.Equal(t, d.length, 0)
		}
	})

}
