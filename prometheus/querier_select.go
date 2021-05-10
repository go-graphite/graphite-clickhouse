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

type set interface {
	toSeriesSet() storage.SeriesSet
	toChunkSeriesSet() storage.ChunkSeriesSet
}

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
func (q *Querier) Select(sortSerie bool, selectHints *storage.SelectHints, labelsMatcher ...*labels.Matcher) *seriesSet {
	var from, until time.Time
	ss := emptySeriesSet()

	if from.IsZero() && selectHints != nil && selectHints.Start != 0 {
		from = time.Unix(selectHints.Start/1000, (selectHints.Start%1000)*1000000)
	}
	if until.IsZero() && selectHints != nil && selectHints.End != 0 {
		until = time.Unix(selectHints.End/1000, (selectHints.End%1000)*1000000)
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
		ss.warnings = append(ss.warnings, err)
		return ss
	}

	if am.Len() == 0 {
		return ss
	}

	if selectHints == nil {
		// /api/v1/series?match[]=...
		return newMetricsSet(am.DisplayNames())
	}

	var step int64 = 60000
	if selectHints.Step != 0 {
		step = selectHints.Step
	}

	maxDataPoints := (until.Unix() - from.Unix()) / (step / 1000)

	fetchRequests := data.MultiFetchRequest{
		data.TimeFrame{
			From:          from.Unix(),
			Until:         until.Unix(),
			MaxDataPoints: maxDataPoints,
		}: &data.Targets{List: []string{}, AM: am},
	}
	reply, err := fetchRequests.Fetch(q.ctx, q.config, config.ContextPrometheus)
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
