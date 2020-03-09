// +build !noprom

package prometheus

import (
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

func (q *Querier) lookup(from, until time.Time, labelsMatcher ...*labels.Matcher) (*alias.Map, error) {
	terms, err := makeTaggedFromPromQL(labelsMatcher)
	if err != nil {
		return nil, err
	}
	fndResult, err := finder.FindTagged(q.config, q.ctx, terms, from.Unix(), until.Unix())

	if err != nil {
		return nil, err
	}

	am := alias.New()
	am.Merge(fndResult)
	return am, nil
}

// Select returns a set of series that matches the given label matchers.
func (q *Querier) Select(selectParams *storage.SelectParams, labelsMatcher ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	var from, until time.Time

	if from.IsZero() && selectParams != nil && selectParams.Start != 0 {
		from = time.Unix(selectParams.Start/1000, (selectParams.Start%1000)*1000000)
	}
	if until.IsZero() && selectParams != nil && selectParams.End != 0 {
		until = time.Unix(selectParams.End/1000, (selectParams.End%1000)*1000000)
	}

	if from.IsZero() && q.mint > 0 {
		from = time.Unix(q.mint/1000, (q.mint%1000)*1000000)
	}
	if until.IsZero() && q.maxt > 0 {
		until = time.Unix(q.maxt/1000, (q.maxt%1000)*1000000)
	}

	if until.IsZero() {
		until = time.Now()
	}
	if from.IsZero() {
		from = until.AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays)
	}

	am, err := q.lookup(from, until, labelsMatcher...)
	if err != nil {
		return nil, nil, err
	}

	if am.Len() == 0 {
		return emptySeriesSet(), nil, nil
	}

	if selectParams == nil {
		// /api/v1/series?match[]=...
		return newMetricsSet(am.DisplayNames()), nil, nil
	}

	pointsTable, isReverse, rollupRules := render.SelectDataTable(q.config, from.Unix(), until.Unix(), []string{}, config.ContextPrometheus)
	if pointsTable == "" {
		return nil, nil, fmt.Errorf("data table is not specified")
	}

	wr := where.New()
	wr.And(where.In("Path", am.Series(isReverse)))
	wr.And(where.TimestampBetween("Time", from.Unix(), until.Unix()+1))

	pw := where.New()
	pw.And(where.DateBetween("Date", from, until))

	query := fmt.Sprintf(render.QUERY,
		pointsTable, pw.PreWhereSQL(), wr.SQL(),
	)

	body, err := clickhouse.Reader(
		scope.WithTable(q.ctx, pointsTable),
		q.config.ClickHouse.Url,
		query,
		clickhouse.Options{Timeout: q.config.ClickHouse.DataTimeout.Value(), ConnectTimeout: q.config.ClickHouse.ConnectTimeout.Value()},
	)

	if err != nil {
		return nil, nil, err
	}

	data, err := render.DataParse(body, nil, isReverse)
	if err != nil {
		return nil, nil, err
	}

	data.Points.Sort()
	data.Points.Uniq()

	if data.Points.Len() == 0 {
		return emptySeriesSet(), nil, nil
	}

	ss, err := makeSeriesSet(data, am, rollupRules)
	if err != nil {
		return nil, nil, err
	}

	return ss, nil, nil
}
