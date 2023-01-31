//go:build !noprom
// +build !noprom

package prometheus

import (
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/render/data"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// override in unit tests for stable results
var timeNow = time.Now

func (q *Querier) lookup(from, until int64, labelsMatcher ...*labels.Matcher) (*alias.Map, error) {
	terms, err := makeTaggedFromPromQL(labelsMatcher)
	if err != nil {
		return nil, err
	}
	var stat finder.FinderStat
	// TODO: implement use stat for Prometheus queries
	fndResult, err := finder.FindTagged(q.config, q.ctx, terms, from, until, &stat)

	if err != nil {
		return nil, err
	}

	am := alias.New()
	am.Merge(fndResult, false)
	return am, nil
}

func (q *Querier) timeRange(hints *storage.SelectHints) (int64, int64) {
	var from, until time.Time

	// ClickHouse supported range of values by the Date type:  [1970-01-01, 2149-06-06]
	if hints != nil && hints.Start > 0 && hints.Start < 5662310400000 {
		from = time.Unix(hints.Start/1000, (hints.Start%1000)*1000000)
	}
	if hints != nil && hints.End > 0 && hints.End < 5662310400000 {
		until = time.Unix(hints.End/1000, (hints.End%1000)*1000000)
	}

	if until.IsZero() {
		if q.maxt > 0 && q.maxt < 5662310400000 {
			until = time.Unix(q.maxt/1000, (q.maxt%1000)*1000000)
		} else {
			until = timeNow()
		}
	}

	if from.IsZero() {
		if q.mint > 0 && q.mint < 5662310400000 {
			from = time.Unix(q.mint/1000, (q.mint%1000)*1000000)
		} else {
			from = until.AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays)
		}
	}

	return from.Unix(), until.Unix()
}

// Select returns a set of series that matches the given label matchers.
func (q *Querier) Select(sortSeries bool, hints *storage.SelectHints, labelsMatcher ...*labels.Matcher) storage.SeriesSet {
	from, until := q.timeRange(hints)
	am, err := q.lookup(from, until, labelsMatcher...)
	if err != nil {
		return nil //, nil, err @TODO
	}

	if am.Len() == 0 {
		return emptySeriesSet()
	}

	if hints != nil && hints.Func == "series" {
		// /api/v1/series?match[]=...
		return newMetricsSet(am.DisplayNames()) //, nil, nil
	}

	var step int64 = 60000
	if hints.Step != 0 {
		step = hints.Step
	}

	maxDataPoints := 1000 * (until - from) / step

	multiTarget := data.MultiTarget{
		data.TimeFrame{
			From:          from,
			Until:         until,
			MaxDataPoints: maxDataPoints,
		}: data.NewTargets([]string{}, am),
	}
	reply, err := multiTarget.Fetch(q.ctx, q.config, config.ContextPrometheus)
	if err != nil {
		return nil // , nil, err @TODO
	}

	if len(reply) == 0 {
		return emptySeriesSet() //, nil, nil
	}

	ss, err := makeSeriesSet(reply[0].Data)
	if err != nil {
		return nil // , nil, err @TODO
	}

	return ss //, nil, nil
}
