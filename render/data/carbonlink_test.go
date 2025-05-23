package data

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/point"
	graphitePickle "github.com/lomik/graphite-pickle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type carbonlinkMocked struct {
	mock.Mock
}

func (c *carbonlinkMocked) CacheQueryMulti(ctx context.Context, metrics []string) (map[string][]graphitePickle.DataPoint, error) {
	args := c.Called(ctx, metrics)
	return args.Get(0).(map[string][]graphitePickle.DataPoint), args.Error(1)
}

func TestSetCarbonlingClient(t *testing.T) {
	assert.Nil(t, carbonlink, "client is set in the begining of tests")

	cfg := config.New()
	cfg.Carbonlink.Server = "localhost:0"
	setCarbonlinkClient(&cfg.Carbonlink)
	assert.NotNil(t, carbonlink, "client is not set aftert setCarbonlinkClient")
	assert.IsType(t, &graphitePickle.CarbonlinkClient{}, carbonlink.carbonlinkFetcher, "")
	carbonlink = nil
}

func TestQueryCarbonlink(t *testing.T) {
	carbonlink = nil

	res := make(map[string][]graphitePickle.DataPoint)
	metrics := []string{"metric1", "metric2"}
	dataPoints := []graphitePickle.DataPoint{
		{
			Timestamp: 1500000000,
			Value:     13,
		},
		{
			Timestamp: 1500000060,
			Value:     14,
		},
	}

	for _, m := range metrics {
		res[m] = dataPoints
	}

	testGrCarbonlinkClient := new(carbonlinkMocked)
	testGrCarbonlinkClient.On("CacheQueryMulti", mock.AnythingOfType("*context.timerCtx"), metrics).Return(res, nil)
	carbonlink = &carbonlinkClient{testGrCarbonlinkClient, time.Duration(0)}

	now := uint32(time.Now().Unix())
	points := queryCarbonlink(context.Background(), carbonlink, metrics)()
	// Result points.metrics are not ordered
	pMetrics := []string{points.MetricName(1), points.MetricName(2)}
	i := 0

	for _, m := range pMetrics {
		for _, dp := range dataPoints {
			// There is a tiny chance that point will have greated Timestamp than now. Here we test it's at most the next second
			assert.GreaterOrEqual(t, uint32(1), (points.List()[i].Timestamp - now), "difference between now and point.Timestamp is greater than 1")

			expectedPoint := point.Point{MetricID: points.MetricID(m), Value: dp.Value, Time: uint32(dp.Timestamp), Timestamp: points.List()[i].Timestamp}
			assert.Equal(t, expectedPoint, points.List()[i], "point is not correct")

			i++
		}
	}

	sort.Strings(pMetrics)
	assert.Equal(t, metrics, pMetrics, "sorted points.metrics is not the same as in request")

	carbonlink = nil
	emptyPoints := queryCarbonlink(context.Background(), carbonlink, metrics)()
	assert.Nil(t, emptyPoints, "points are not nil")
}
