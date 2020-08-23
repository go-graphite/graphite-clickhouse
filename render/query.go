package render

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"go.uber.org/zap"
)

// table, prewhere, where
const queryUnaggregated = `SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp) FROM %s %s %s GROUP BY Path FORMAT RowBinary`

// TimeFrame contains information about fetch request time conditions
type TimeFrame struct {
	From          int64
	Until         int64
	MaxDataPoints int64
}

type Targets struct {
	List        []string
	AM          *alias.Map
	pointsTable string
	isReverse   bool
	rollupObj   *rollup.Rules
}

// MultiFetchRequest is a map of TimeFrame keys and targets slice of strings values
type MultiFetchRequest map[TimeFrame]*Targets

type CHResponse struct {
	Data  *Data
	From  int64
	Until int64
}

type Reply struct {
	CHResponses []CHResponse
	lock        sync.RWMutex
}

func (r *Reply) Append(chr CHResponse) {
	r.lock.Lock()
	r.CHResponses = append(r.CHResponses, chr)
	r.lock.Unlock()
}

// EmptyResponse is an empty []CHResponse
var EmptyResponse []CHResponse = []CHResponse{{EmptyData, 0, 0}}

// EmptyReply is used when no metrics/data points are read
var EmptyReply *Reply = &Reply{EmptyResponse, sync.RWMutex{}}

// FetchDataPoints fetches the data from ClickHouse and parses it into []CHResponse
func FetchDataPoints(ctx context.Context, cfg *config.Config, fetchRequests MultiFetchRequest, chContext string) (*Reply, error) {
	var lock sync.RWMutex
	var wg sync.WaitGroup
	logger := scope.Logger(ctx)

	reply := &Reply{make([]CHResponse, 0, len(fetchRequests)), sync.RWMutex{}}
	errors := make([]error, 0, len(fetchRequests))

	for tf, targets := range fetchRequests {
		if tf.MaxDataPoints <= 0 {
			tf.MaxDataPoints = int64(cfg.ClickHouse.MaxDataPoints)
		}
		targets.pointsTable, targets.isReverse, targets.rollupObj = SelectDataTable(cfg, tf.From, tf.Until, targets.List, chContext)
		if targets.pointsTable == "" {
			err := fmt.Errorf("data tables is not specified for %v", targets.List[0])
			lock.Lock()
			errors = append(errors, err)
			lock.Unlock()
			logger.Error("data tables is not specified", zap.Error(err))
			return EmptyReply, err
		}
		wg.Add(1)
		go func(tf TimeFrame, targets *Targets) {
			defer wg.Done()
			err := reply.getDataPoints(ctx, cfg, tf, targets)
			if err != nil {
				lock.Lock()
				errors = append(errors, err)
				lock.Unlock()
				return
			}
		}(tf, targets)
	}
	wg.Wait()
	for len(errors) != 0 {
		return EmptyReply, errors[0]
	}

	return reply, nil
}

func (r *Reply) getDataPoints(ctx context.Context, cfg *config.Config, tf TimeFrame, targets *Targets) error {
	logger := scope.Logger(ctx)
	var data *Data
	var err error
	var rollupTime time.Duration
	defer func() {
		if rollupTime > 0 {
			logger.Debug(
				"rollup",
				zap.String("runtime", rollupTime.String()),
				zap.Duration("runtime_ns", rollupTime),
			)
		}
	}()

	data, err = r.getDataUnaggregated(ctx, cfg, tf, targets)

	if err != nil {
		return nil
	}

	logger.Info("data", zap.Int("read_bytes", data.length), zap.Int("read_points", data.Points.Len()))

	sortStart := time.Now()
	data.Points.Sort()
	d := time.Since(sortStart)
	logger.Debug("sort", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

	data.Points.Uniq()
	rollupStart := time.Now()
	err = data.rollupObj.RollupPoints(data.Points, uint32(tf.From))
	if err != nil {
		logger.Error("rollup failed", zap.Error(err))
		return err
	}
	rollupTime += time.Since(rollupStart)

	data.Aliases = targets.AM

	r.Append(CHResponse{
		Data:  data,
		From:  tf.From,
		Until: tf.Until,
	})
	return nil
}

func (r *Reply) getDataUnaggregated(ctx context.Context, cfg *config.Config, tf TimeFrame, targets *Targets) (data *Data, err error) {
	// Generic
	logger := scope.Logger(ctx)
	data = EmptyData

	metricList := targets.AM.Series(targets.isReverse)

	if len(metricList) == 0 {
		return
	}

	// from carbonlink request
	carbonlinkResponseRead := queryCarbonlink(ctx, cfg, metricList)

	now := time.Now().Unix()
	age := dry.Max(0, now-tf.From)

	// end of generic prepare

	// calculate max step

	var maxStep int64
	steps := make(map[string]uint32)
	for _, m := range metricList {
		step, _ := targets.rollupObj.Lookup(m, uint32(age))
		steps[m] = step
		if int64(step) > maxStep {
			maxStep = int64(step)
		}
	}
	until := dry.CeilToMultiplier(tf.Until, maxStep) - 1

	pw := where.New()
	pw.And(where.DateBetween("Date", time.Unix(tf.From, 0), time.Unix(until, 0)))

	wr := where.New()
	wr.And(where.In("Path", metricList))

	wr.And(where.TimestampBetween("Time", tf.From, until))

	query := fmt.Sprintf(
		queryUnaggregated,
		targets.pointsTable, pw.PreWhereSQL(), wr.SQL(),
	)

	body, err := clickhouse.Reader(
		scope.WithTable(ctx, targets.pointsTable),
		cfg.ClickHouse.Url,
		query,
		clickhouse.Options{Timeout: cfg.ClickHouse.DataTimeout.Value(), ConnectTimeout: cfg.ClickHouse.ConnectTimeout.Value()},
	)

	if err != nil {
		logger.Error("reader", zap.Error(err))
		return
	}

	// fetch carbonlink response
	carbonlinkData := carbonlinkResponseRead()

	parseStart := time.Now()

	// pass carbonlinkData to data parser
	data, err = parseUnaggregatedResponse(body, carbonlinkData, targets.isReverse)
	if err != nil {
		logger.Error("data", zap.Error(err), zap.Int("read_bytes", data.length))
		return nil, err
	}
	d := time.Since(parseStart)

	data.rollupObj = targets.rollupObj
	data.Points.SetSteps(steps)

	logger.Debug("parse", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

	return
}
