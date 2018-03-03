package render

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/stretchr/testify/assert"
)

func makeData(points []point.Point) []byte {
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
		assert.Equal(t, []point.Point{}, d.Points)
	})

	t.Run("ok", func(t *testing.T) {
		table := [][]point.Point{
			{
				{"hello.world", 1520056686, 42.1, 1520056706},
			},
			{
				{"hello.world", 1520056686, 42.1, 1520056706},
				{"foobar", 1520056687, 42.2, 1520056707},
			},
			{
				{"samelen1", 1520056686, 42.1, 1520056706},
				{"samelen2", 1520056687, 42.2, 1520056707},
			},
			{
				{"key1", 1520056686, 42.1, 1520056706},
				{"key2", 1520056687, 42.2, 1520056707},
				{"key1", 1520056687, 42.2, 1520056707},
			},
		}

		for i := 0; i < len(table); i++ {
			t.Run(fmt.Sprintf("ok #%d", i), func(t *testing.T) {
				body := makeData(table[i])

				r := bytes.NewReader(body)

				d, err := DataParse(r, nil, false)
				assert.NoError(t, err)
				assert.Equal(t, table[i], d.Points)
			})
		}
	})

	t.Run("incomplete response", func(t *testing.T) {
		body := makeData([]point.Point{
			{
				Metric:    "hello.world",
				Time:      1520056686,
				Value:     42.1,
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
