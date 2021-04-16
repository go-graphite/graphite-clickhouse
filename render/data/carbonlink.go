package data

import (
	"context"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"go.uber.org/zap"

	graphitePickle "github.com/lomik/graphite-pickle"
)

// carbonlink to get data from carbonlink server globally
type carbonlinkClient struct {
	graphitePickle.CarbonlinkClient
	totalTimeout time.Duration
}

var carbonlink *carbonlinkClient = nil

// setCarbonlinkClient setup the client once. Does nothing if Config.Carbonlink.Server is not set
func setCarbonlinkClient(config *config.Carbonlink) {
	if carbonlink != nil {
		return
	}
	if config.Server == "" {
		return
	}
	carbonlink = &carbonlinkClient{
		*graphitePickle.NewCarbonlinkClient(
			config.Server,
			config.Retries,
			config.Threads,
			config.ConnectTimeout.Value(),
			config.QueryTimeout.Value(),
		),
		config.TotalTimeout.Value(),
	}
	return
}

// queryCarbonlink returns callable result fetcher
func queryCarbonlink(parentCtx context.Context, metrics []string) func() *point.Points {
	logger := scope.Logger(parentCtx)
	if carbonlink == nil {
		return func() *point.Points { return nil }
	}

	carbonlinkResponseChan := make(chan *point.Points, 1)

	fetchResult := func() *point.Points {
		result := <-carbonlinkResponseChan
		return result
	}

	go func() {
		ctx, cancel := context.WithTimeout(parentCtx, carbonlink.totalTimeout)
		defer cancel()

		res, err := carbonlink.CacheQueryMulti(ctx, metrics)

		if err != nil {
			logger.Info("carbonlink failed", zap.Error(err))
		}

		result := point.NewPoints()

		if res != nil && len(res) > 0 {
			tm := uint32(time.Now().Unix())

			for metric, points := range res {
				metricID := result.MetricID(metric)
				for _, p := range points {
					result.AppendPoint(metricID, p.Value, uint32(p.Timestamp), tm)
				}
			}
		}

		carbonlinkResponseChan <- result
	}()

	return fetchResult
}
