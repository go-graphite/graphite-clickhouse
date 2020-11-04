package render

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"go.uber.org/zap"
)

// from, until, step, function, table, prewhere, where
// arrayFilter(x->isNotNull(x)) - do not pass nulls to client
// -Resample - group time and values by time intervals and apply aggregation function
// -OrNull - if there aren't points in an interval, null will be returned
// intDiv(Time, x)*x - round Time down to step multiplier
const queryAggregated = `SELECT Path,
	arrayFilter(x->isNotNull(x), anyOrNullResample(%[1]d, %[2]d, %[3]d)(toUInt32(intDiv(Time, %[3]d)*%[3]d), Time)),
	arrayFilter(x->isNotNull(x), %[4]sOrNullResample(%[1]d, %[2]d, %[3]d)(Value, Time))
FROM %[5]s
%[6]s
%[7]s
GROUP BY Path
FORMAT RowBinary`

// table, prewhere, where
const queryUnaggregated = `SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp) FROM %s %s %s GROUP BY Path FORMAT RowBinary`

// TimeFrame contains information about fetch request time conditions
type TimeFrame struct {
	From          int64
	Until         int64
	MaxDataPoints int64
}

type Targets struct {
	// List contains list of metrics in one the target
	List        []string
	AM          *alias.Map
	pointsTable string
	isReverse   bool
	rollupObj   *rollup.Rules
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
			return fmt.Errorf("metrics limit exceeded: %v < %v", num, t.AM.Len())
		}
	}
	return nil
}

type CHResponse struct {
	Data  *Data
	From  int64
	Until int64
}

type Reply struct {
	CHResponses []CHResponse
	lock        sync.RWMutex
	cStep       *commonStep
}

func (r *Reply) Append(chr CHResponse) {
	r.lock.Lock()
	r.CHResponses = append(r.CHResponses, chr)
	r.lock.Unlock()
}

// EmptyResponse is an empty []CHResponse
var EmptyResponse []CHResponse = []CHResponse{{EmptyData, 0, 0}}

// EmptyReply is used when no metrics/data points are read
var EmptyReply *Reply = &Reply{EmptyResponse, sync.RWMutex{}, nil}

// FetchDataPoints fetches the data from ClickHouse and parses it into []CHResponse
func FetchDataPoints(ctx context.Context, cfg *config.Config, fetchRequests MultiFetchRequest, chContext string) (*Reply, error) {
	var lock sync.RWMutex
	var wg sync.WaitGroup
	logger := scope.Logger(ctx)

	err := fetchRequests.checkMetricsLimitExceeded(cfg.Common.MaxMetricsPerTarget)
	if err != nil {
		logger.Error("data fetch", zap.Error(err))
		return nil, err
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, cfg.ClickHouse.DataTimeout.Duration)
	defer cancel()
	cStep := &commonStep{
		result: 0,
		wg:     sync.WaitGroup{},
		lock:   sync.RWMutex{},
	}

	if cfg.ClickHouse.InternalAggregation {
		cStep.addTargets(len(fetchRequests))
	}

	reply := &Reply{
		CHResponses: make([]CHResponse, 0, len(fetchRequests)),
		lock:        sync.RWMutex{},
		cStep:       cStep,
	}
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
			err := reply.getDataPoints(ctxTimeout, cfg, tf, targets)
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

	if cfg.ClickHouse.InternalAggregation {
		data, err = r.getDataAggregated(ctx, cfg, tf, targets)
	} else {
		data, err = r.getDataUnaggregated(ctx, cfg, tf, targets)
	}

	if err != nil {
		return err
	}

	logger.Info("data", zap.Int("read_bytes", data.length), zap.Int("read_points", data.Points.Len()))

	// ClickHouse returns sorted and uniq values, when internal aggregation is used
	// But if carbonlink is used, we still need to sort, filter and rollup points
	if !cfg.ClickHouse.InternalAggregation || carbonlinkClient(cfg) != nil {
		sortStart := time.Now()
		data.Points.Sort()
		d := time.Since(sortStart)
		logger.Debug("sort", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

		data.Points.Uniq()
		rollupStart := time.Now()
		err = data.rollupObj.RollupPoints(data.Points, tf.From, data.commonStep)
		if err != nil {
			logger.Error("rollup failed", zap.Error(err))
			return err
		}
		rollupTime += time.Since(rollupStart)
	}

	data.Aliases = targets.AM

	r.Append(CHResponse{
		Data:  data,
		From:  tf.From,
		Until: tf.Until,
	})
	return nil
}

func (r *Reply) getDataAggregated(ctx context.Context, cfg *config.Config, tf TimeFrame, targets *Targets) (data *Data, err error) {
	// Generic prepare part
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

	maxDataPoints := dry.Min(tf.MaxDataPoints, int64(cfg.ClickHouse.MaxDataPoints))
	// map for points.SetAggregations
	metricsAggregation := make(map[string][]string)
	// map of CH external tables body grouped by aggregation function
	bodyAggregation := make(map[string][]byte)

	// Grouping metrics by aggregation steps and functions
	var step int64
	for _, m := range metricList {
		newStep, agg := targets.rollupObj.Lookup(m, uint32(age))
		step = r.cStep.calculateUnsafe(step, int64(newStep))
		if mm, ok := bodyAggregation[agg.Name()]; ok {
			bodyAggregation[agg.Name()] = append(mm, []byte("\n"+m)...)
		} else {
			bodyAggregation[agg.Name()] = []byte(m)
		}

		if targets.isReverse {
			m = reverse.String(m)
		}
		if mm, ok := metricsAggregation[agg.Name()]; ok {
			metricsAggregation[agg.Name()] = append(mm, m)
		} else {
			metricsAggregation[agg.Name()] = []string{m}
		}
	}
	r.cStep.calculate(step)
	step = dry.Max(r.cStep.getResult(), (tf.Until-tf.From)/maxDataPoints)
	step = dry.CeilToMultiplier(step, r.cStep.getResult())

	b := make(chan io.ReadCloser, len(metricsAggregation))
	e := make(chan error)
	queryWg := sync.WaitGroup{}
	queryContext, cancel := context.WithCancel(ctx)

	defer func() {
		cancel()
		queryWg.Wait()
		close(e)
		close(b)
	}()

	for agg, tableBody := range bodyAggregation {
		from := dry.CeilToMultiplier(tf.From, step)
		until := dry.CeilToMultiplier(tf.Until, step) - 1
		pw := where.New()
		pw.And(where.DateBetween("Date", time.Unix(from, 0), time.Unix(until, 0)))

		wr := where.New()
		tempTable := clickhouse.ExternalTable{
			Name: "metrics_list",
			Columns: []clickhouse.Column{{
				Name: "Path",
				Type: "String",
			}},
			Format: "TSV",
			Data:   tableBody,
		}
		extData := clickhouse.NewExternalData(tempTable)
		extData.SetDebug(cfg.Debug.Directory, cfg.Debug.ExternalDataPerm.FileMode)
		wr.And(where.InTable("Path", tempTable.Name))

		wr.And(where.TimestampBetween("Time", from, until))
		query := fmt.Sprintf(
			queryAggregated,
			from, until, step, agg,
			targets.pointsTable, pw.PreWhereSQL(), wr.SQL(),
		)
		queryWg.Add(1)
		go func(query string) {
			defer queryWg.Done()
			body, err := clickhouse.Reader(
				scope.WithTable(ctx, targets.pointsTable),
				cfg.ClickHouse.Url,
				query,
				clickhouse.Options{
					Timeout:        cfg.ClickHouse.DataTimeout.Value(),
					ConnectTimeout: cfg.ClickHouse.ConnectTimeout.Value(),
				},
				extData,
			)
			if err != nil {
				logger.Error("reader", zap.Error(err))
				select {
				case <-queryContext.Done():
					return
				case e <- err:
					return
				}
			}
			select {
			case <-queryContext.Done():
				return
			case b <- body:
				return
			}
		}(query)
	}

	carbonlinkData := carbonlinkResponseRead()
	data, err = parseAggregatedResponse(ctx, b, e, carbonlinkData, targets.isReverse)
	if err != nil {
		logger.Error("data", zap.Error(err), zap.Int("read_bytes", data.length))
		return nil, err
	}
	data.commonStep = step
	data.Points.SetAggregations(metricsAggregation)
	data.rollupObj = targets.rollupObj
	return
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
	metricsAggregation := make(map[string][]string)
	for _, m := range metricList {
		step, agg := targets.rollupObj.Lookup(m, uint32(age))
		if int64(step) > maxStep {
			maxStep = int64(step)
		}

		if targets.isReverse {
			m = reverse.String(m)
		}

		steps[m] = step
		if mm, ok := metricsAggregation[agg.Name()]; ok {
			metricsAggregation[agg.Name()] = append(mm, m)
		} else {
			metricsAggregation[agg.Name()] = []string{m}
		}
	}
	until := dry.CeilToMultiplier(tf.Until, maxStep) - 1

	tableBody := []byte(strings.Join(metricList, "\n"))
	tempTable := clickhouse.ExternalTable{
		Name: "metrics_list",
		Columns: []clickhouse.Column{{
			Name: "Path",
			Type: "String",
		}},
		Format: "TSV",
		Data:   tableBody,
	}
	extData := clickhouse.NewExternalData(tempTable)
	extData.SetDebug(cfg.Debug.Directory, cfg.Debug.ExternalDataPerm.FileMode)

	pw := where.New()
	pw.And(where.DateBetween("Date", time.Unix(tf.From, 0), time.Unix(until, 0)))

	wr := where.New()
	wr.And(where.InTable("Path", tempTable.Name))

	wr.And(where.TimestampBetween("Time", tf.From, until))

	query := fmt.Sprintf(
		queryUnaggregated,
		targets.pointsTable, pw.PreWhereSQL(), wr.SQL(),
	)

	body, err := clickhouse.Reader(
		scope.WithTable(ctx, targets.pointsTable),
		cfg.ClickHouse.Url,
		query,
		clickhouse.Options{
			Timeout:        cfg.ClickHouse.DataTimeout.Value(),
			ConnectTimeout: cfg.ClickHouse.ConnectTimeout.Value(),
		},
		extData,
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
	data.Points.SetAggregations(metricsAggregation)

	logger.Debug("parse", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

	return
}
