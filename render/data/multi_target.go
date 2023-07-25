package data

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/errs"
	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

// TimeFrame contains information about fetch request time conditions
type TimeFrame struct {
	From          int64
	Until         int64
	MaxDataPoints int64
}

// MultiTarget is a map of TimeFrame keys and targets slice of strings values
type MultiTarget map[TimeFrame]*Targets

func MFRToMultiTarget(v3Request *v3pb.MultiFetchRequest) MultiTarget {
	multiTarget := make(MultiTarget)

	if len(v3Request.Metrics) > 0 {
		for _, m := range v3Request.Metrics {
			tf := TimeFrame{
				From:          m.StartTime,
				Until:         m.StopTime,
				MaxDataPoints: m.MaxDataPoints,
			}
			if _, ok := multiTarget[tf]; ok {
				target := multiTarget[tf]
				target.Append(m.PathExpression)
			} else {
				multiTarget[tf] = NewTargetsOne(m.PathExpression, len(v3Request.Metrics), alias.New())
			}
		}
	}
	return multiTarget
}

func (m *MultiTarget) checkMetricsLimitExceeded(num int) error {
	if num <= 0 {
		// zero or negative means unlimited
		return nil
	}
	for _, t := range *m {
		if num < t.AM.Len() {
			return errs.NewErrorWithCode(fmt.Sprintf("metrics limit exceeded: %d < %d", num, t.AM.Len()), http.StatusForbidden)
		}
	}
	return nil
}

func getDataTimeout(cfg *config.Config, m *MultiTarget) time.Duration {
	dataTimeout := cfg.ClickHouse.DataTimeout
	if len(cfg.ClickHouse.QueryParams) > 1 {
		var maxDuration time.Duration
		for tf := range *m {
			duration := time.Second * time.Duration(tf.Until-tf.From)
			if duration >= maxDuration {
				maxDuration = duration
			}
		}

		n := config.GetQueryParam(cfg.ClickHouse.QueryParams, maxDuration)
		return cfg.ClickHouse.QueryParams[n].DataTimeout
	}

	return dataTimeout
}

func GetQueryLimiter(username string, cfg *config.Config, m *MultiTarget) (string, limiter.ServerLimiter) {
	n := 0
	if username != "" && len(cfg.ClickHouse.UserLimits) > 0 {
		if u, ok := cfg.ClickHouse.UserLimits[username]; ok {
			return username, u.Limiter
		}
	}

	if len(cfg.ClickHouse.QueryParams) > 1 {
		var maxDuration time.Duration
		for tf := range *m {
			duration := time.Second * time.Duration(tf.Until-tf.From)
			if duration >= maxDuration {
				maxDuration = duration
			}
		}

		n = config.GetQueryParam(cfg.ClickHouse.QueryParams, maxDuration)
	}

	return "", cfg.ClickHouse.QueryParams[n].Limiter
}

func GetQueryLimiterFrom(username string, cfg *config.Config, from, until int64) limiter.ServerLimiter {
	n := 0
	if username != "" && len(cfg.ClickHouse.UserLimits) > 0 {
		if u, ok := cfg.ClickHouse.UserLimits[username]; ok {
			return u.Limiter
		}
	}

	if len(cfg.ClickHouse.QueryParams) > 1 {
		n = config.GetQueryParam(cfg.ClickHouse.QueryParams, time.Second*time.Duration(until-from))
	}

	return cfg.ClickHouse.QueryParams[n].Limiter
}

func GetQueryParam(username string, cfg *config.Config, m *MultiTarget) (*config.QueryParam, int) {
	n := 0

	if len(cfg.ClickHouse.QueryParams) > 1 {
		var maxDuration time.Duration
		for tf := range *m {
			duration := time.Second * time.Duration(tf.Until-tf.From)
			if duration >= maxDuration {
				maxDuration = duration
			}
		}

		n = config.GetQueryParam(cfg.ClickHouse.QueryParams, maxDuration)
	}

	return &cfg.ClickHouse.QueryParams[n], n
}

// Fetch fetches the parsed ClickHouse data returns CHResponses
func (m *MultiTarget) Fetch(ctx context.Context, cfg *config.Config, chContext string, qlimiter limiter.ServerLimiter, queueDuration *time.Duration) (CHResponses, error) {
	var (
		lock    sync.RWMutex
		wg      sync.WaitGroup
		entered int
	)
	logger := scope.Logger(ctx)
	setCarbonlinkClient(&cfg.Carbonlink)

	err := m.checkMetricsLimitExceeded(cfg.Common.MaxMetricsPerTarget)
	if err != nil {
		logger.Error("data fetch", zap.Error(err))
		return nil, err
	}

	dataTimeout := getDataTimeout(cfg, m)

	ctxTimeout, cancel := context.WithTimeout(ctx, dataTimeout)
	defer func() {
		for i := 0; i < entered; i++ {
			qlimiter.Leave(ctxTimeout, "render")
		}
		cancel()
	}()

	errors := make([]error, 0, len(*m))
	query := newQuery(cfg, len(*m))

	for tf, targets := range *m {
		tf, targets := tf, targets
		cond := &conditions{TimeFrame: &tf,
			Targets:           targets,
			aggregated:        cfg.ClickHouse.InternalAggregation,
			appendEmptySeries: cfg.Common.AppendEmptySeries,
		}
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
		if qlimiter.Enabled() {
			start := time.Now()
			err = qlimiter.Enter(ctxTimeout, "render")
			*queueDuration += time.Since(start)
			if err != nil {
				// status = http.StatusServiceUnavailable
				// queueFail = true
				// http.Error(w, err.Error(), status)
				lock.Lock()
				errors = append(errors, err)
				lock.Unlock()
				break
			}
			entered++
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
