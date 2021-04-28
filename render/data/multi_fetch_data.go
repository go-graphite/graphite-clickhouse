package data

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"go.uber.org/zap"
)

// TimeFrame contains information about fetch request time conditions
type TimeFrame struct {
	From          int64
	Until         int64
	MaxDataPoints int64
}

// MultiFetchRequest is a map of TimeFrame keys and targets slice of strings values
type MultiFetchRequest map[TimeFrame]*Targets

func (m *MultiFetchRequest) checkMetricsLimitExceeded(num int) error {
	if num <= 0 {
		// zero or negative means unlimited
		return nil
	}
	for _, t := range *m {
		if num < t.AM.Len() {
			return clickhouse.NewErrorWithCode(fmt.Sprintf("metrics limit exceeded: %d < %d", num, t.AM.Len()), http.StatusForbidden)
		}
	}
	return nil
}

// Fetch fetches the parsed ClickHouse data returns CHResponses
func (m *MultiFetchRequest) Fetch(ctx context.Context, cfg *config.Config, chContext string) (CHResponses, error) {
	var lock sync.RWMutex
	var wg sync.WaitGroup
	logger := scope.Logger(ctx)
	setCarbonlinkClient(&cfg.Carbonlink)

	err := m.checkMetricsLimitExceeded(cfg.Common.MaxMetricsPerTarget)
	if err != nil {
		logger.Error("data fetch", zap.Error(err))
		return nil, err
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, cfg.ClickHouse.DataTimeout.Duration)
	defer cancel()

	errors := make([]error, 0, len(*m))
	query := newQuery(cfg, len(*m))

	for tf, targets := range *m {
		tf, targets := tf, targets
		cond := &conditions{TimeFrame: &tf, Targets: targets, aggregated: cfg.ClickHouse.InternalAggregation}
		if cond.MaxDataPoints <= 0 || int64(cfg.ClickHouse.MaxDataPoints) < cond.MaxDataPoints {
			cond.MaxDataPoints = int64(cfg.ClickHouse.MaxDataPoints)
		}
		err := cond.selectDataTable(cfg, cond.TimeFrame, chContext)
		if err != nil {
			lock.Lock()
			errors = append(errors, err)
			lock.Unlock()
			logger.Error("data tables is not specified", zap.Error(err))
			return EmptyResponse(), err
		}
		wg.Add(1)
		go func(cond *conditions) {
			defer wg.Done()
			err := query.getDataPoints(ctxTimeout, cond)
			if err != nil {
				lock.Lock()
				errors = append(errors, err)
				lock.Unlock()
				return
			}
		}(cond)
	}
	wg.Wait()
	for len(errors) != 0 {
		return EmptyResponse(), errors[0]
	}

	return query.CHResponses, nil
}
