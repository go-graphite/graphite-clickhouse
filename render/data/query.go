package data

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
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

// CHResponse contains the parsed Data and From/Until timestamps
type CHResponse struct {
	Data  *Data
	From  int64
	Until int64
}

// CHResponses is a slice of CHResponse
type CHResponses []CHResponse

// EmptyResponse returns an CHResponses with one element containing emptyData for the following encoding
func EmptyResponse() CHResponses { return CHResponses{{emptyData, 0, 0}} }

type query struct {
	CHResponses
	cStep       *commonStep
	chConfig    *config.ClickHouse
	debugConfig *config.Debug
	lock        sync.RWMutex
	aggregated  bool
}

func newQuery(cfg *config.Config, targets int) *query {
	cStep := &commonStep{
		result: 0,
		wg:     sync.WaitGroup{},
		lock:   sync.RWMutex{},
	}

	if cfg.ClickHouse.InternalAggregation {
		cStep.addTargets(targets)
	}

	query := &query{
		CHResponses: make([]CHResponse, 0, targets),
		cStep:       cStep,
		chConfig:    &cfg.ClickHouse,
		debugConfig: &cfg.Debug,
		lock:        sync.RWMutex{},
		aggregated:  cfg.ClickHouse.InternalAggregation,
	}

	return query
}

func (q *query) appendReply(chr CHResponse) {
	q.lock.Lock()
	q.CHResponses = append(q.CHResponses, chr)
	q.lock.Unlock()
}

func (q *query) getDataPoints(ctx context.Context, tf TimeFrame, targets *Targets) error {
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

	if q.aggregated {
		data, err = q.getDataAggregated(ctx, tf, targets)
	} else {
		data, err = q.getDataUnaggregated(ctx, tf, targets)
	}

	if err != nil {
		return err
	}

	logger.Info("data", zap.Int("read_bytes", data.length), zap.Int("read_points", data.Points.Len()))

	// ClickHouse returns sorted and uniq values, when internal aggregation is used
	// But if carbonlink is used, we still need to sort, filter and rollup points
	if !q.aggregated || carbonlink != nil {
		sortStart := time.Now()
		data.Points.Sort()
		d := time.Since(sortStart)
		logger.Debug("sort", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

		data.Points.Uniq()
		rollupStart := time.Now()
		err = targets.rollupObj.RollupPoints(data.Points, tf.From, data.commonStep)
		if err != nil {
			logger.Error("rollup failed", zap.Error(err))
			return err
		}
		rollupTime += time.Since(rollupStart)
	}

	data.Aliases = targets.AM

	q.appendReply(CHResponse{
		Data:  data,
		From:  tf.From,
		Until: tf.Until,
	})
	return nil
}

func (q *query) getDataAggregated(ctx context.Context, tf TimeFrame, targets *Targets) (data *Data, err error) {
	// Generic prepare part
	logger := scope.Logger(ctx)
	data = emptyData

	metricList := targets.AM.Series(targets.isReverse)
	if len(metricList) == 0 {
		return
	}

	var metricListUnreverse []string
	if targets.isReverse {
		metricListUnreverse = targets.AM.Series(false)
	} else {
		metricListUnreverse = metricList
	}

	var metricListRuleLookup []string
	if targets.isReverse && targets.rollupUseReverted {
		metricListRuleLookup = metricListUnreverse
	} else {
		metricListRuleLookup = metricList
	}

	// from carbonlink request
	carbonlinkResponseRead := queryCarbonlink(ctx, carbonlink, metricListUnreverse)

	now := time.Now().Unix()
	age := dry.Max(0, now-tf.From)

	// end of generic prepare

	// map for points.SetAggregations
	metricsAggregation := make(map[string][]string)
	// map of CH external tables body grouped by aggregation function
	bodyAggregation := make(map[string]*strings.Builder)

	// Grouping metrics by aggregation steps and functions
	var step int64
	for n, m := range metricList {
		newStep, agg := targets.rollupObj.Lookup(metricListRuleLookup[n], uint32(age))
		step = q.cStep.calculateUnsafe(step, int64(newStep))
		if mm, ok := bodyAggregation[agg.Name()]; ok {
			mm.WriteString(m + "\n")
		} else {
			var mm strings.Builder
			mm.WriteString(m + "\n")
			bodyAggregation[agg.Name()] = &mm
		}

		if targets.isReverse {
			m = metricListUnreverse[n]
		}

		if mm, ok := metricsAggregation[agg.Name()]; ok {
			metricsAggregation[agg.Name()] = append(mm, m)
		} else {
			metricsAggregation[agg.Name()] = []string{m}
		}
	}
	q.cStep.calculate(step)
	step = dry.Max(q.cStep.getResult(), dry.Ceil(tf.Until-tf.From, tf.MaxDataPoints))
	step = dry.CeilToMultiplier(step, q.cStep.getResult())

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
		until := dry.FloorToMultiplier(tf.Until, step) + step - 1
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
			Data:   []byte(tableBody.String()),
		}
		extData := clickhouse.NewExternalData(tempTable)
		extData.SetDebug(q.debugConfig.Directory, q.debugConfig.ExternalDataPerm.FileMode)
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
				q.chConfig.Url,
				query,
				clickhouse.Options{
					Timeout:        q.chConfig.DataTimeout.Value(),
					ConnectTimeout: q.chConfig.ConnectTimeout.Value(),
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

	carbonlinkPoints := carbonlinkResponseRead()
	data, err = parseAggregatedResponse(ctx, b, e, carbonlinkPoints, targets.isReverse)
	if err != nil {
		logger.Error("data", zap.Error(err), zap.Int("read_bytes", data.length))
		return nil, err
	}
	data.commonStep = step
	data.Points.SetAggregations(metricsAggregation)
	return
}

func (q *query) getDataUnaggregated(ctx context.Context, tf TimeFrame, targets *Targets) (data *Data, err error) {
	// Generic
	logger := scope.Logger(ctx)
	data = emptyData

	metricList := targets.AM.Series(targets.isReverse)
	if len(metricList) == 0 {
		return
	}

	var metricListUnreverse []string
	if targets.isReverse {
		metricListUnreverse = targets.AM.Series(false)
	} else {
		metricListUnreverse = metricList
	}

	var metricListRuleLookup []string
	if targets.isReverse && targets.rollupUseReverted {
		metricListRuleLookup = metricListUnreverse
	} else {
		metricListRuleLookup = metricList
	}

	// from carbonlink request
	carbonlinkResponseRead := queryCarbonlink(ctx, carbonlink, metricListUnreverse)

	now := time.Now().Unix()
	age := dry.Max(0, now-tf.From)

	// end of generic prepare

	// calculate max step

	var maxStep int64
	steps := make(map[string]uint32)
	metricsAggregation := make(map[string][]string)
	for n, m := range metricListRuleLookup {
		step, agg := targets.rollupObj.Lookup(m, uint32(age))
		if int64(step) > maxStep {
			maxStep = int64(step)
		}

		if targets.isReverse {
			m = metricListUnreverse[n]
		}

		steps[m] = step
		if mm, ok := metricsAggregation[agg.Name()]; ok {
			metricsAggregation[agg.Name()] = append(mm, m)
		} else {
			metricsAggregation[agg.Name()] = []string{m}
		}
	}
	until := dry.FloorToMultiplier(tf.Until, maxStep) + maxStep - 1

	tableBody := []byte(strings.Join(metricList, "\n") + "\n")
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
	extData.SetDebug(q.debugConfig.Directory, q.debugConfig.ExternalDataPerm.FileMode)

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
		q.chConfig.Url,
		query,
		clickhouse.Options{
			Timeout:        q.chConfig.DataTimeout.Value(),
			ConnectTimeout: q.chConfig.ConnectTimeout.Value(),
		},
		extData,
	)

	if err != nil {
		logger.Error("reader", zap.Error(err))
		return
	}

	// fetch carbonlink response
	carbonlinkPoints := carbonlinkResponseRead()

	parseStart := time.Now()

	// pass carbonlinkData to data parser
	data, err = parseUnaggregatedResponse(body, carbonlinkPoints, targets.isReverse)
	if err != nil {
		logger.Error("data", zap.Error(err), zap.Int("read_bytes", data.length))
		return nil, err
	}
	d := time.Since(parseStart)

	data.Points.SetSteps(steps)
	data.Points.SetAggregations(metricsAggregation)

	logger.Debug("parse", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

	return
}
