package data

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/errs"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

// from, until, step, function, table, prewhere, where
// arrayFilter(x->isNotNull(x)) - do not pass nulls to client
// -Resample - group time and values by time intervals and apply aggregation function
// -OrNull - if there aren't points in an interval, null will be returned
// intDiv(Time, x)*x - round Time down to step multiplier
// TODO: support custom aggregating functions
const queryAggregated = `WITH anyResample(%[1]d, %[2]d, %[3]d)(toUInt32(intDiv(Time, %[3]d)*%[3]d), Time) AS mask
SELECT Path,
 arrayFilter(m->m!=0, mask) AS times,
 arrayFilter((v,m)->m!=0, %[4]sResample(%[1]d, %[2]d, %[3]d)(Value, Time), mask) AS values
FROM %[5]s
%[6]s
%[7]s
GROUP BY Path
FORMAT RowBinary`

// table, prewhere, where
const queryUnaggregated = `SELECT Path, groupArray(Time), groupArray(Value), groupArray(Timestamp)
FROM %s
%s
%s
GROUP BY Path
FORMAT RowBinary`

// name of external-data table with metrics paths
const extTableName = "metrics_list"

type query struct {
	CHResponses
	cStep         *commonStep
	chTLSConfig   *tls.Config
	chQueryParams []config.QueryParam

	chConnectTimeout time.Duration
	debugDir         string
	debugExtDataPerm os.FileMode
	lock             sync.RWMutex
}

type conditions struct {
	*TimeFrame
	*Targets
	// aggregated shows is it request with ClickHouse aggregation or not
	aggregated bool
	// step is used in requests for proper until/from calculation. It's max(steps) for non-aggregated
	// requests and LCM(steps) for aggregated requests
	step int64
	// from is aligned to step
	from int64
	// until is aligned to step
	until int64
	// metricUnreversed grouped by step
	steps map[uint32][]string
	// prewhere contains PREWHERE condition
	prewhere string
	// where contains WHERE condition
	where string
	// show list of NaN values instead of empty results
	appendEmptySeries bool
	// metricUnreversed grouped by aggregating function
	aggregations map[string][]string
	// External-data bodies grouped by aggregatig function. For non-aggregated requests "" used as a key
	extDataBodies    map[string]*strings.Builder
	metricsRequested []string
	metricsUnreverse []string
	metricsLookup    []string
	appliedFunctions map[string][]string
}

func newQuery(cfg *config.Config, targets int) *query {
	var cStep *commonStep = nil
	if cfg.ClickHouse.InternalAggregation {
		cStep = &commonStep{
			result: 0,
			wg:     sync.WaitGroup{},
			lock:   sync.RWMutex{},
		}

		cStep.addTargets(targets)
	}

	query := &query{
		CHResponses:      make([]CHResponse, 0, targets),
		cStep:            cStep,
		chQueryParams:    cfg.ClickHouse.QueryParams,
		chConnectTimeout: cfg.ClickHouse.ConnectTimeout,
		chTLSConfig:      cfg.ClickHouse.TLSConfig,
		debugDir:         cfg.Debug.Directory,
		debugExtDataPerm: cfg.Debug.ExternalDataPerm,
		lock:             sync.RWMutex{},
	}

	return query
}

func (q *query) appendReply(chr CHResponse) {
	q.lock.Lock()
	q.CHResponses = append(q.CHResponses, chr)
	q.lock.Unlock()
}

func (q *query) getParam(from, until int64) (string, time.Duration) {
	duration := time.Second * time.Duration(until-from)

	n := config.GetQueryParam(q.chQueryParams, duration)

	return q.chQueryParams[n].URL, q.chQueryParams[n].DataTimeout
}

func (q *query) getDataPoints(ctx context.Context, cond *conditions) error {
	logger := scope.Logger(ctx)
	var err error

	cond.prepareMetricsLists()
	if len(cond.metricsRequested) == 0 {
		q.cStep.doneTarget()
		return nil
	}

	// carbonlink request
	carbonlinkResponseRead := queryCarbonlink(ctx, carbonlink, cond.metricsUnreverse)

	err = cond.prepareLookup()
	if err != nil {
		logger.Error("prepare_lookup", zap.Error(err))
		return errs.NewErrorWithCode(err.Error(), http.StatusBadRequest)
	}
	cond.setStep(q.cStep)
	if cond.step < 1 {
		return ErrSetStepTimeout
	}
	cond.setFromUntil()
	cond.setPrewhere()
	cond.setWhere()

	queryContext, queryCancel := context.WithCancel(ctx)
	defer queryCancel()
	data := prepareData(queryContext, len(cond.extDataBodies), carbonlinkResponseRead)

	var ch_read_bytes, ch_read_rows int64
	for agg, extTableBody := range cond.extDataBodies {
		extData := q.metricsListExtData(extTableBody)
		query := cond.generateQuery(agg)
		data.wg.Add(1)
		go func() {
			defer data.wg.Done()
			chURL, chDataTimeout := q.getParam(cond.from, cond.until)
			body, err := clickhouse.Reader(
				scope.WithTable(ctx, cond.pointsTable),
				chURL,
				query,
				clickhouse.Options{
					Timeout:        chDataTimeout,
					ConnectTimeout: q.chConnectTimeout,
					TLSConfig:      q.chTLSConfig,
				},
				extData,
			)
			if err == nil {
				atomic.AddInt64(&ch_read_bytes, body.ChReadBytes())
				atomic.AddInt64(&ch_read_rows, body.ChReadRows())
				err = data.parseResponse(queryContext, body, cond)
				if err != nil {
					logger.Error("reader", zap.Error(err))
					data.e <- err
					queryCancel()
				}
			} else {
				logger.Error("reader", zap.Error(err))
				data.e <- err
				queryCancel()
			}
		}()
	}

	err = data.wait(queryContext)
	metrics.SendQueryRead(cond.queryMetrics, cond.from, cond.until, data.spent.Milliseconds(), int64(data.Points.Len()), int64(data.length), ch_read_rows, ch_read_bytes, err != nil)
	if err != nil {
		logger.Error(
			"data_parser", zap.Error(err), zap.Int("read_bytes", data.length),
			zap.String("runtime", data.spent.String()), zap.Duration("runtime_ns", data.spent),
		)
		return err
	}
	logger.Info(
		"data_parse", zap.Int("read_bytes", data.length), zap.Int("read_points", data.Points.Len()),
		zap.String("runtime", data.spent.String()), zap.Duration("runtime_ns", data.spent),
	)

	data.setSteps(cond)
	data.Points.SetAggregations(cond.aggregations)

	// ClickHouse returns sorted and uniq values, when internal aggregation is used
	// But if carbonlink is used, we still need to sort, filter and rollup points
	if !cond.aggregated || carbonlink != nil {
		sortStart := time.Now()
		data.Points.Sort()
		d := time.Since(sortStart)
		logger.Debug("sort", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

		data.Points.Uniq()
		rollupStart := time.Now()
		err = cond.rollupRules.RollupPoints(data.Points, cond.From, data.CommonStep)
		if err != nil {
			logger.Error("rollup failed", zap.Error(err))
			return err
		}
		rollupTime := time.Since(rollupStart)
		logger.Debug(
			"rollup",
			zap.String("runtime", rollupTime.String()),
			zap.Duration("runtime_ns", rollupTime),
		)
	}

	data.AM = cond.AM

	q.appendReply(CHResponse{
		Data:                 data.Data,
		From:                 cond.From,
		Until:                cond.Until,
		AppendOutEmptySeries: cond.appendEmptySeries,
		AppliedFunctions:     cond.appliedFunctions,
	})
	return nil
}

func (q *query) metricsListExtData(body *strings.Builder) *clickhouse.ExternalData {
	extTable := clickhouse.ExternalTable{
		Name: extTableName,
		Columns: []clickhouse.Column{{
			Name: "Path",
			Type: "String",
		}},
		Format: "TSV",
		Data:   []byte(body.String()),
	}

	extData := clickhouse.NewExternalData(extTable)
	extData.SetDebug(q.debugDir, q.debugExtDataPerm)
	return extData
}

func (c *conditions) prepareMetricsLists() {
	c.metricsUnreverse = c.AM.Series(false)
	c.metricsRequested = c.metricsUnreverse

	if c.isReverse {
		c.metricsRequested = make([]string, len(c.metricsRequested))
		for i := range c.metricsRequested {
			c.metricsRequested[i] = reverse.String(c.metricsUnreverse[i])
		}
	}

	c.metricsLookup = c.metricsRequested
	if c.rollupUseReverted {
		c.metricsLookup = c.metricsUnreverse
	}
}

func (c *conditions) prepareLookup() error {
	age := uint32(dry.Max(0, time.Now().Unix()-c.From))
	c.aggregations = make(map[string][]string)
	c.appliedFunctions = make(map[string][]string)
	c.extDataBodies = make(map[string]*strings.Builder)
	c.steps = make(map[uint32][]string)
	aggName := ""

	for i := range c.metricsRequested {
		step, agg, _, _ := c.rollupRules.Lookup(c.metricsLookup[i], age, false)

		// Override agregation with an argument of consolidateBy function.
		// consolidateBy with its argument is passed through FilteringFunctions field of carbonapi_v3_pb protocol.
		// Currently it just finds the first target matching the metric
		// to avoid making multiple request for every type of aggregation for a given metric.
		for _, alias := range c.AM.Get(c.metricsUnreverse[i]) {
			requestedAgg, err := c.GetRequestedAggregation(alias.Target)
			if err != nil {
				return fmt.Errorf("failed to choose appropriate aggregation for '%s': %s", alias.Target, err.Error())
			}
			if requestedAgg != "" {
				agg = rollup.AggrMap[requestedAgg]
				c.appliedFunctions[alias.Target] = []string{graphiteConsolidationFunction}
				break
			}
		}

		if _, ok := c.steps[step]; !ok {
			c.steps[step] = make([]string, 0)
		}
		// Fill up metric names for steps only for non-aggregated requests.
		// Aggregated use commonStep
		if !c.aggregated {
			c.steps[step] = append(c.steps[step], c.metricsUnreverse[i])
		}

		// Fill up metric names for aggregations
		if mm, ok := c.aggregations[agg.Name()]; ok {
			c.aggregations[agg.Name()] = append(mm, c.metricsUnreverse[i])
		} else {
			c.aggregations[agg.Name()] = []string{c.metricsUnreverse[i]}
		}

		// Build external-data bodies. For non-aggregated requests there is only one request
		if c.aggregated {
			aggName = agg.Name()
		}
		if mm, ok := c.extDataBodies[aggName]; ok {
			mm.WriteString(c.metricsRequested[i] + "\n")
		} else {
			var mm strings.Builder
			c.extDataBodies[aggName] = &mm
			mm.WriteString(c.metricsRequested[i] + "\n")
		}
	}
	return nil
}

var ErrSetStepTimeout = errors.New("unexpected error, setStep timeout")

func (c *conditions) setStep(cStep *commonStep) {
	step := int64(0)
	if !c.aggregated {
		// Use max(steps)
		for s := range c.steps {
			step = dry.Max(step, int64(s))
		}
		c.step = step
		return
	}

	// Use LCM(steps)
	// XXX: This could cause problems, when MutliFetchRequest uses different MaxDataPoints,
	// but currently (2021-04-22) it's not possible
	for s := range c.steps {
		step = cStep.calculateUnsafe(step, int64(s))
	}
	cStep.calculate(step)
	rStep := cStep.getResult()
	if rStep == -1 {
		c.step = -1
		return
	}
	step = dry.Max(rStep, dry.Ceil(c.Until-c.From, c.MaxDataPoints))
	c.step = dry.CeilToMultiplier(step, rStep)
	return
}

func (c *conditions) setFromUntil() {
	c.from = dry.CeilToMultiplier(c.From, c.step)
	c.until = dry.FloorToMultiplier(c.Until, c.step) + c.step - 1
}

func (c *conditions) setPrewhere() {
	pw := where.New()
	pw.And(where.DateBetween("Date", c.from, c.until))
	c.prewhere = pw.PreWhereSQL()
}

func (c *conditions) setWhere() {
	wr := where.New()
	wr.And(where.InTable("Path", extTableName))
	wr.And(where.TimestampBetween("Time", c.from, c.until))
	c.where = wr.SQL()
}

func (c *conditions) generateQuery(agg string) string {
	if c.aggregated {
		return c.generateQueryaAggregated(agg)
	}
	return c.generateQueryUnaggregated()
}

func (c *conditions) generateQueryaAggregated(agg string) string {
	return fmt.Sprintf(
		queryAggregated,
		c.from, c.until, c.step, agg,
		c.pointsTable, c.prewhere, c.where,
	)
}

func (c *conditions) generateQueryUnaggregated() string {
	return fmt.Sprintf(queryUnaggregated, c.pointsTable, c.prewhere, c.where)
}
