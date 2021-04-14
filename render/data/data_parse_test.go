package data

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/lomik/graphite-clickhouse/helper/point"
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

func makeAggregatedBody(points []testPoint) []byte {
	buf := new(bytes.Buffer)
	w := RowBinary.NewEncoder(buf)

	for i := 0; i < len(points); i++ {
		w.String(points[i].Metric)
		w.NullableUint32List(points[i].PointValues.Times)
		w.NullableFloat64List(points[i].PointValues.Values)
	}

	return buf.Bytes()
}

func makeUnaggregatedBody(points []testPoint) []byte {
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

func TestUnaggregatedDataParse(t *testing.T) {
	t.Run("empty response", func(t *testing.T) {
		body := []byte{}
		r := bytes.NewReader(body)

		d, err := parseUnaggregatedResponse(r, nil, false)
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
				{"key1", &pointValues{[]float64{42.1, 42.2}, []uint32{1520056686, 1520056687}, []uint32{1520056706, 1520056687}}},
				{"key2", &pointValues{[]float64{42.2}, []uint32{1520056687}, []uint32{1520056707}}},
			},
		}

		for i := 0; i < len(table); i++ {
			t.Run(fmt.Sprintf("ok #%d", i), func(t *testing.T) {
				body := makeUnaggregatedBody(table[i])

				r := bytes.NewReader(body)

				d, err := parseUnaggregatedResponse(r, nil, false)
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
		body := makeUnaggregatedBody([]testPoint{
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

		_, err := parseUnaggregatedResponse(r, nil, false)
		assert.Error(t, err)
	})

	t.Run("incomplete response", func(t *testing.T) {
		body := makeUnaggregatedBody([]testPoint{
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

			d, err := parseUnaggregatedResponse(r, nil, false)
			assert.Error(t, err)
			assert.Equal(t, d.length, 0)
		}
	})
}

func TestAggregatedDataParse(t *testing.T) {
	ctx := context.Background()
	t.Run("empty response", func(t *testing.T) {
		body := []byte{}
		b := make(chan io.ReadCloser, 1)
		go func() {
			b <- ioutil.NopCloser(bytes.NewReader(body))
			close(b)
		}()

		d, err := parseAggregatedResponse(ctx, b, nil, nil, false)
		assert.NoError(t, err)
		assert.Empty(t, d.Points.List())
	})

	t.Run("stop on error in channel", func(t *testing.T) {
		body := makeAggregatedBody([]testPoint{
			{"hello.world", &pointValues{[]float64{42.1}, []uint32{1520056686}, []uint32{1520056706}}},
		})

		b := make(chan io.ReadCloser, 3)
		e := make(chan error)
		initialError := fmt.Errorf("Some error")
		go func() {
			b <- ioutil.NopCloser(bytes.NewReader(body))
			for len(b) != 0 {
				// sleep until parseAggregatedResponse reads the channel to avoid false negative
				time.Sleep(time.Millisecond)
			}
			e <- initialError
			b <- ioutil.NopCloser(bytes.NewReader(body))
			close(b)
		}()

		d, err := parseAggregatedResponse(ctx, b, e, nil, false)
		assert.Error(t, err)
		assert.Equal(t, initialError, err)
		assert.Equal(t, 1, len(d.Points.List()))
		assert.Equal(t, point.Point{1, 42.1, 1520056686, 1520056686}, d.Points.List()[0])
	})

	t.Run("incomplete response", func(t *testing.T) {
		body := makeAggregatedBody([]testPoint{
			{
				Metric: "hello.world",
				PointValues: &pointValues{
					Values: []float64{42.1},
					Times:  []uint32{1520056686},
				},
			},
		})

		for i := 1; i < len(body)-1; i++ {
			b := make(chan io.ReadCloser, 1)
			var wg sync.WaitGroup
			go func() {
				wg.Add(1)
				b <- ioutil.NopCloser(bytes.NewReader(body[:i]))
				wg.Wait()
				close(b)
			}()

			d, err := parseAggregatedResponse(ctx, b, nil, nil, false)
			wg.Done()
			assert.Error(t, err)
			assert.Equal(t, 0, d.length)
		}
	})

	t.Run("malformed ClickHouse body", func(t *testing.T) {
		testPoints := []testPoint{
			{
				// different length of -Resample arrays
				Metric: "different.arrays.in.body",
				PointValues: &pointValues{
					Values: []float64{42.1},
					Times:  []uint32{1520056706, 1520056707},
				},
			},
			{
				// different length of result arrays even with the same -Resample results
				Metric: "hello.world",
				PointValues: &pointValues{
					Values: []float64{42.1, math.NaN()},
					Times:  []uint32{1520056706, 1520056707},
				},
			},
			{
				// All null times/values are filtered by arrayFilter(isNotNull(x))
				Metric: "null.in.the.middle",
				PointValues: &pointValues{
					Values: []float64{42.1, math.NaN(), 43},
					Times:  []uint32{1520056686, RowBinary.NullUint32, 1520056690},
				},
			},
		}

		b := make(chan io.ReadCloser, 1)
		for _, point := range testPoints {

			go func(p testPoint) {
				b <- ioutil.NopCloser(bytes.NewReader(makeAggregatedBody([]testPoint{p})))
			}(point)

			_, err := parseAggregatedResponse(ctx, b, nil, nil, false)
			assert.Error(t, err)
		}
		close(b)
	})

	t.Run("normal work", func(t *testing.T) {
		points := []testPoint{
			{
				Metric: "hello.world",
				PointValues: &pointValues{
					Values: []float64{42.1},
					Times:  []uint32{1520056686},
				},
			},
			{
				Metric: "null.in.the.middle",
				PointValues: &pointValues{
					Values: []float64{42.1, 43},
					Times:  []uint32{1520056686, 1520056690},
				},
			},
		}

		b := make(chan io.ReadCloser, 2)
		var wg sync.WaitGroup
		go func() {
			wg.Add(1)
			for _, p := range points {
				body := makeAggregatedBody([]testPoint{p})
				b <- ioutil.NopCloser(bytes.NewReader(body))
			}
			wg.Wait()
			close(b)
		}()

		d, err := parseAggregatedResponse(ctx, b, nil, nil, false)
		wg.Done()
		result := []point.Point{
			{1, 42.1, 1520056686, 1520056686},
			{2, 42.1, 1520056686, 1520056686},
			{2, 43, 1520056690, 1520056690},
		}
		assert.NoError(t, err)
		assert.Equal(t, result, d.Points.List())
	})

	t.Run("timeout", func(t *testing.T) {
		// Set length of bodies 2, but writes only one of them,
		// then use context with timeout to raise deadline error
		points := []testPoint{
			{
				Metric: "hello.world",
				PointValues: &pointValues{
					Values: []float64{42.1},
					Times:  []uint32{1520056686},
				},
			},
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		b := make(chan io.ReadCloser, 2)
		var wg sync.WaitGroup
		go func() {
			wg.Add(1)
			body := makeAggregatedBody(points)
			b <- ioutil.NopCloser(bytes.NewReader(body))
			wg.Wait()
			close(b)
		}()

		d, err := parseAggregatedResponse(ctxTimeout, b, nil, nil, false)
		wg.Done()
		result := []point.Point{
			{1, 42.1, 1520056686, 1520056686},
		}
		assert.Error(t, err, "context deadline exceeded")
		assert.Equal(t, result, d.Points.List())
	})
}

func TestPrepare(t *testing.T) {
	t.Run("empty datapoints", func(t *testing.T) {
		assert.Equal(t, &Data{Points: point.NewPoints()}, prepare(nil))
	})

	t.Run("data contains points", func(t *testing.T) {
		//points := []point.Point{{1, 42.1, 1520056686, 1520056686}}
		extraPoints := point.NewPoints()
		extraPoints.MetricID("some.metric1")
		extraPoints.MetricID("some.metric2")
		extraPoints.AppendPoint(1, 1, 3, 3)
		extraPoints.AppendPoint(2, 1, 3, 3)
		d := prepare(extraPoints)
		assert.Equal(t, []point.Point{{1, 1, 3, 3}, {2, 1, 3, 3}}, d.Points.List())
		assert.Equal(t, "some.metric1", d.Points.MetricName(1))
		assert.Equal(t, "some.metric2", d.Points.MetricName(2))
	})
}
