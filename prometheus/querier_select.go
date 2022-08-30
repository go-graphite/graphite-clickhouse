//go:build !noprom
// +build !noprom

package prometheus

import (
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/render/data"
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
	am.Merge(fndResult, false)
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
	// ClickHouse supported Datetime range of values: [1970-01-01 00:00:00, 2105-12-31 23:59:59]
	if until.IsZero() && q.maxt > 0 && q.maxt <= 4291765199000 {
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

	var step int64 = 60000
	if selectParams.Step != 0 {
		step = selectParams.Step
	}

	maxDataPoints := (until.Unix() - from.Unix()) / (step / 1000)

	multiTarget := data.MultiTarget{
		data.TimeFrame{
			From:          from.Unix(),
			Until:         until.Unix(),
			MaxDataPoints: maxDataPoints,
		}: &data.Targets{List: []string{}, AM: am},
	}
	reply, err := multiTarget.Fetch(q.ctx, q.config, config.ContextPrometheus)
	if err != nil {
		return nil, nil, err
	}

	if len(reply) == 0 {
		return emptySeriesSet(), nil, nil
	}

	ss, err := makeSeriesSet(reply[0].Data)
	if err != nil {
		return nil, nil, err
	}

	return ss, nil, nil
}
