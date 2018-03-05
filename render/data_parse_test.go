package render

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/stretchr/testify/assert"
)

type testPoint struct {
	Metric    string
	Value     float64
	Time      uint32
	Timestamp uint32
}

func makeData(points []testPoint) []byte {
	buf := new(bytes.Buffer)
	w := RowBinary.NewEncoder(buf)

	for i := 0; i < len(points); i++ {
		w.String(points[i].Metric)
		w.Uint32(uint32(points[i].Time))
		w.Float64(points[i].Value)
		w.Uint32(uint32(points[i].Timestamp))
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
				{"hello.world", 42.1, 1520056686, 1520056706},
			},
			{
				{"hello.world", 42.1, 1520056686, 1520056706},
				{"foobar", 42.2, 1520056687, 1520056707},
			},
			{
				{"samelen1", 42.1, 1520056686, 1520056706},
				{"samelen2", 42.2, 1520056687, 1520056707},
			},
			{
				{"key1", 42.1, 1520056686, 1520056706},
				{"key2", 42.2, 1520056687, 1520056707},
				{"key1", 42.2, 1520056687, 1520056707},
			},
		}

		for i := 0; i < len(table); i++ {
			t.Run(fmt.Sprintf("ok #%d", i), func(t *testing.T) {
				body := makeData(table[i])

				r := bytes.NewReader(body)

				d, err := DataParse(r, nil, false)
				assert.NoError(t, err)
				for j := 0; j < len(table[i]); j++ {
					assert.Equal(t, table[i][j].Metric, d.Points.MetricName(d.Points.List()[j].MetricID))
					assert.Equal(t, table[i][j].Time, d.Points.List()[j].Time)
					assert.Equal(t, table[i][j].Value, d.Points.List()[j].Value)
					assert.Equal(t, table[i][j].Timestamp, d.Points.List()[j].Timestamp)
				}
			})
		}
	})

	t.Run("incomplete response", func(t *testing.T) {
		body := makeData([]testPoint{
			{
				Metric:    "hello.world",
				Value:     42.1,
				Time:      1520056686,
				Timestamp: 1520056706,
			},
		})

		for i := 1; i < len(body)-1; i++ {
			r := bytes.NewReader(body[:i])

			d, err := DataParse(r, nil, false)
			assert.Error(t, err)
			assert.Nil(t, d)
		}
	})

}
