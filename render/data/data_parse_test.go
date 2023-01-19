package data

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
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
		w.Uint32List(points[i].PointValues.Times)
		w.Float64List(points[i].PointValues.Values)
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

func testCarbonlinkReaderNil() *point.Points {
	return nil
}

func TestUnaggregatedDataParse(t *testing.T) {
	ctx := context.Background()
	cond := &conditions{Targets: &Targets{isReverse: false}, aggregated: false}
	t.Run("empty response", func(t *testing.T) {
		body := []byte{}
		r := io.NopCloser(bytes.NewReader(body))
		d := prepareData(ctx, 1, testCarbonlinkReaderNil)

		err := d.parseResponse(ctx, r, cond)
		assert.NoError(t, err)
		werr := d.wait(ctx)
		assert.NoError(t, werr)
		assert.Empty(t, d.Points.List())
	})

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
			{"long.metric.with.points.key1", &pointValues{[]float64{42.1, 42.2}, []uint32{1520056686, 1520056687}, []uint32{1520056706, 1520056687}}},
			{"long.metric.with.points.key2", &pointValues{[]float64{42.2}, []uint32{1520056687}, []uint32{1520056707}}},
		},
	}

	for i := 0; i < len(table); i++ {
		t.Run(fmt.Sprintf("ok #%d", i), func(t *testing.T) {
			body := makeUnaggregatedBody(table[i])

			r := io.NopCloser(bytes.NewReader(body))
			d := prepareData(ctx, 1, testCarbonlinkReaderNil)

			err := d.parseResponse(ctx, r, cond)
			assert.NoError(t, err)
			werr := d.wait(ctx)
			assert.NoError(t, werr)
			// point number
			p := 0
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
	for i := 0; i < len(table); i++ {
		t.Run(fmt.Sprintf("reversed #%d", i), func(t *testing.T) {
			cond := &conditions{Targets: &Targets{isReverse: true}, aggregated: false}
			body := makeUnaggregatedBody(table[i])

			r := io.NopCloser(bytes.NewReader(body))
			d := prepareData(ctx, 1, testCarbonlinkReaderNil)

			err := d.parseResponse(ctx, r, cond)
			assert.NoError(t, err)
			werr := d.wait(ctx)
			assert.NoError(t, werr)
			// point number
			p := 0
			for j := 0; j < len(table[i]); j++ {
				for m := 0; m < len(table[i][j].PointValues.Times); m++ {
					assert.Equal(t, table[i][j].Metric, reverse.String(d.Points.MetricName(d.Points.List()[p].MetricID)))
					assert.Equal(t, table[i][j].PointValues.Times[m], d.Points.List()[p].Time)
					assert.Equal(t, table[i][j].PointValues.Values[m], d.Points.List()[p].Value)
					assert.Equal(t, table[i][j].PointValues.Timestamps[m], d.Points.List()[p].Timestamp)
					p++
				}
			}
		})
	}

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
		r := io.NopCloser(bytes.NewReader(body))
		d := prepareData(ctx, 1, testCarbonlinkReaderNil)

		err := d.parseResponse(ctx, r, cond)
		assert.Error(t, err)
	})

	t.Run("incomplete response", func(t *testing.T) {
		points := []testPoint{
			{
				Metric: "hello.world",
				PointValues: &pointValues{
					Values:     []float64{42.1},
					Times:      []uint32{1520056686},
					Timestamps: []uint32{1520056706},
				},
			},
			{
				Metric: "bye-bye.sky",
				PointValues: &pointValues{
					Values:     []float64{42.42},
					Times:      []uint32{1520056686},
					Timestamps: []uint32{1520056706},
				},
			},
		}
		body := makeUnaggregatedBody(points)

		firstMetricLength := len(makeUnaggregatedBody(points[:1]))
		for i := 1; i < len(body)-1; i++ {
			if i == firstMetricLength {
				// length of the first metric
				continue
			}
			r := io.NopCloser(bytes.NewReader(body[:i]))
			d := prepareData(ctx, 1, testCarbonlinkReaderNil)

			err := d.parseResponse(ctx, r, cond)
			assert.Error(t, err)
			assert.True(t, (d.length == 0 || d.length == firstMetricLength), "length of read data is wrong")
		}
	})
}

func TestAggregatedDataParse(t *testing.T) {
	ctx := context.Background()
	cond := &conditions{Targets: &Targets{isReverse: false}, aggregated: true}
	t.Run("empty response", func(t *testing.T) {
		body := []byte{}
		d := prepareData(ctx, 1, testCarbonlinkReaderNil)
		r := io.NopCloser(bytes.NewReader(body))

		err := d.parseResponse(ctx, r, cond)
		assert.NoError(t, err)
		assert.Empty(t, d.Points.List())
	})

	t.Run("incomplete response", func(t *testing.T) {
		points := []testPoint{
			{
				Metric: "hello.world",
				PointValues: &pointValues{
					Values: []float64{42.1},
					Times:  []uint32{1520056686},
				},
			},
			{
				Metric: "bye-bye.sky",
				PointValues: &pointValues{
					Values: []float64{42.1},
					Times:  []uint32{1520056686},
				},
			},
		}
		body := makeAggregatedBody(points)

		firstMetricLength := len(makeAggregatedBody(points[:1]))
		for i := 1; i < len(body)-1; i++ {
			if i == firstMetricLength {
				// length of the first metric
				continue
			}
			r := io.NopCloser(bytes.NewReader(body[:i]))

			d := prepareData(ctx, 1, testCarbonlinkReaderNil)
			err := d.parseResponse(ctx, r, cond)
			assert.Error(t, err)
			assert.True(t, (d.length == 0 || d.length == firstMetricLength), "length of read data is wrong")
		}
	})

	t.Run("malformed ClickHouse body", func(t *testing.T) {
		points := []testPoint{
			{
				// different length of -Resample arrays
				Metric: "different.arrays.in.body",
				PointValues: &pointValues{
					Values: []float64{42.1},
					Times:  []uint32{1520056706, 1520056707},
				},
			},
		}
		body := makeAggregatedBody(points)
		r := io.NopCloser(bytes.NewReader(body))

		d := prepareData(ctx, 1, testCarbonlinkReaderNil)
		err := d.parseResponse(ctx, r, cond)
		assert.Error(t, err)
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
		body := makeAggregatedBody(points)
		r := io.NopCloser(bytes.NewReader(body))

		d := prepareData(ctx, 1, testCarbonlinkReaderNil)
		err := d.parseResponse(ctx, r, cond)
		result := []point.Point{
			{MetricID: 1, Value: 42.1, Time: 1520056686, Timestamp: 1520056686},
			{MetricID: 2, Value: 42.1, Time: 1520056686, Timestamp: 1520056686},
			{MetricID: 2, Value: 43, Time: 1520056690, Timestamp: 1520056690},
		}
		assert.NoError(t, err)
		assert.Equal(t, result, d.Points.List())
	})

	t.Run("reversed", func(t *testing.T) {
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
		body := makeAggregatedBody(points)
		r := io.NopCloser(bytes.NewReader(body))
		cond := &conditions{Targets: &Targets{isReverse: true}, aggregated: true}

		d := prepareData(ctx, 1, testCarbonlinkReaderNil)
		err := d.parseResponse(ctx, r, cond)
		assert.NoError(t, err)
		assert.Equal(t, "world.hello", d.Points.MetricName(1))
		assert.Equal(t, "middle.the.in.null", d.Points.MetricName(2))
	})
}

func TestPrepareDataParse(t *testing.T) {
	ctx := context.Background()
	t.Run("empty datapoints", func(t *testing.T) {
		data := prepareData(ctx, 1, testCarbonlinkReaderNil)
		err := data.wait(ctx)
		assert.NoError(t, err)
		assert.Equal(t, &Data{Points: point.NewPoints()}, data.Data)
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		data := prepareData(ctx, 1, testCarbonlinkReaderNil)
		err := data.wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, &Data{Points: point.NewPoints()}, data.Data)
	})

	t.Run("data contains points", func(t *testing.T) {
		//points := []point.Point{{1, 42.1, 1520056686, 1520056686}}
		extraPoints := point.NewPoints()
		extraPoints.MetricID("some.metric1")
		extraPoints.MetricID("some.metric2")
		extraPoints.AppendPoint(1, 1, 3, 3)
		extraPoints.AppendPoint(2, 1, 3, 3)
		reader := func() *point.Points {
			time.Sleep(1 * time.Millisecond)
			return extraPoints
		}
		d := prepareData(ctx, 1, reader)
		err := d.wait(ctx)
		assert.NoError(t, err)
		assert.Equal(
			t, []point.Point{
				{MetricID: 1, Value: 1, Time: 3, Timestamp: 3},
				{MetricID: 2, Value: 1, Time: 3, Timestamp: 3},
			},
			d.Points.List(),
		)
		assert.Equal(t, "some.metric1", d.Points.MetricName(1))
		assert.Equal(t, "some.metric2", d.Points.MetricName(2))
	})
}

func TestAsyncDataParse(t *testing.T) {
	ctx := context.Background()
	cond := &conditions{Targets: &Targets{isReverse: false}, aggregated: false}

	// normal work is tested in other places
	t.Run("context deadline exceeded", func(t *testing.T) {
		extraPoints := point.NewPoints()
		extraPoints.MetricID("some.metric1")
		extraPoints.MetricID("some.metric2")
		extraPoints.AppendPoint(1, 1, 3, 3)
		extraPoints.AppendPoint(2, 1, 3, 3)
		reader := func() *point.Points { return extraPoints }
		ctx, cancel := context.WithTimeout(ctx, -1*time.Nanosecond)
		defer cancel()
		d := prepareData(ctx, 1, reader)
		assert.Len(t, d.Points.List(), 0, "timeout should prevent points parsing")

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
		r := io.NopCloser(bytes.NewReader(body))

		err := d.parseResponse(ctx, r, cond)
		assert.ErrorIs(t, err, context.DeadlineExceeded, "parseResponse shouldn't return error on a done context")
		assert.Len(t, d.Points.List(), 0, "timeout should prevent points parsing")
		err = d.wait(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded, "data.wait returns 'context dedline exceeded'")
	})

	t.Run("context deadline faster than carbonlink reader", func(t *testing.T) {
		extraPoints := point.NewPoints()
		extraPoints.MetricID("some.metric1")
		extraPoints.MetricID("some.metric2")
		extraPoints.AppendPoint(1, 1, 3, 3)
		extraPoints.AppendPoint(2, 1, 3, 3)
		reader := func() *point.Points {
			time.Sleep(1 * time.Second)
			return extraPoints
		}
		ctx, cancel := context.WithTimeout(ctx, 50*time.Nanosecond)
		defer cancel()
		d := prepareData(ctx, 1, reader)
		err := d.wait(ctx)
		assert.Len(t, d.Points.List(), 0, "timeout should prevent points parsing")
		assert.ErrorIs(t, err, context.DeadlineExceeded, "data.wait returns 'context dedline exceeded'")
	})

	t.Run("cancel context before different steps", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		body := []byte{}
		// works fine
		d := prepareData(ctx, 1, testCarbonlinkReaderNil)
		r := io.NopCloser(bytes.NewReader(body))
		err := d.parseResponse(ctx, r, cond)
		assert.NoError(t, err)
		err = d.wait(ctx)
		assert.NoError(t, err)
		cancel()
		// fails after context is cancelled
		err = d.wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		r = io.NopCloser(bytes.NewReader(body))
		err = d.parseResponse(ctx, r, cond)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("wait fails on errors", func(t *testing.T) {
		d := prepareData(ctx, 1, testCarbonlinkReaderNil)
		d.e <- clickhouse.ErrClickHouseResponse
		err := d.wait(ctx)
		assert.ErrorIs(t, err, clickhouse.ErrClickHouseResponse, "err %v is not expected", err)
	})
}
