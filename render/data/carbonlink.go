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
var carbonlink *graphitePickle.CarbonlinkClient = nil

// carbonlinkClient returns *graphitePickle.CarbonlinkClient. If it's unset, checks if it's configured and initialize it
func carbonlinkClient(c *config.Config) *graphitePickle.CarbonlinkClient {
	if carbonlink != nil {
		return carbonlink
	}
	if c.Carbonlink.Server == "" {
		return nil
	}
	carbonlink = graphitePickle.NewCarbonlinkClient(
		c.Carbonlink.Server,
		c.Carbonlink.Retries,
		c.Carbonlink.Threads,
		c.Carbonlink.ConnectTimeout.Value(),
		c.Carbonlink.QueryTimeout.Value(),
	)
	return carbonlink
}

// queryCarbonlink returns callable result fetcher
func queryCarbonlink(parentCtx context.Context, config *config.Config, metrics []string) func() *point.Points {
	logger := scope.Logger(parentCtx)
	if carbonlinkClient(config) == nil {
		return func() *point.Points { return nil }
	}

	carbonlinkResponseChan := make(chan *point.Points, 1)

	fetchResult := func() *point.Points {
		result := <-carbonlinkResponseChan
		return result
	}

	go func() {
		ctx, cancel := context.WithTimeout(parentCtx, config.Carbonlink.TotalTimeout.Value())
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
