package prometheus

import (
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

func (q *Querier) lookup(from, until time.Time, labelsMatcher ...*labels.Matcher) (map[string][]string, Labeler, error) {
	var labeler Labeler
	var err error
	var fndResult finder.Result

	plainGraphite := makePlainGraphiteQuery(labelsMatcher...)

	if plainGraphite != nil {
		labeler = plainGraphite
		fndResult, err = finder.Find(q.config, q.ctx, plainGraphite.Target(), from.Unix(), until.Unix())
	} else {
		terms, err := makeTaggedFromPromQL(labelsMatcher)
		if err != nil {
			return nil, nil, err
		}
		fndResult, err = finder.FindTagged(q.config, q.ctx, terms, from.Unix(), until.Unix())
	}
	if err != nil {
		return nil, nil, err
	}

	aliases := make(map[string][]string)

	fndSeries := fndResult.Series()
	for i := 0; i < len(fndSeries); i++ {
		key := string(fndSeries[i])
		abs := string(fndResult.Abs(fndSeries[i]))
		if x, ok := aliases[key]; ok {
			aliases[key] = append(x, abs)
		} else {
			aliases[key] = []string{abs}
		}
	}

	return aliases, labeler, nil
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

	aliases, labeler, err := q.lookup(from, until, labelsMatcher...)
	if err != nil {
		return nil, nil, err
	}

	if len(aliases) == 0 {
		return emptySeriesSet(), nil, nil
	}

	if selectParams == nil {
		// /api/v1/series?match[]=...
		resultMetrics := make([]string, 0, len(aliases))
		for _, v := range aliases {
			for _, a := range v {
				resultMetrics = append(resultMetrics, a)
			}
		}
		return newMetricsSet(resultMetrics, labeler), nil, nil
	}

	pointsTable, isReverse, rollupRules := render.SelectDataTable(q.config, from.Unix(), until.Unix(), []string{}, config.ContextPrometheus)
	if pointsTable == "" {
		return nil, nil, fmt.Errorf("data table is not specified")
	}

	selectMetrics := make([]string, 0, len(aliases))
	for m := range aliases {
		if isReverse {
			selectMetrics = append(selectMetrics, reverse.String(m))
		} else {
			selectMetrics = append(selectMetrics, m)
		}
	}

	w := where.New()
	w.And(where.In("Path", selectMetrics))
	w.Andf("Time >= %d AND Time <= %d", from.Unix(), until.Unix()+1)

	preWhere := where.New()
	preWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		from.Format("2006-01-02"),
		until.Format("2006-01-02"),
	)

	query := fmt.Sprintf(`SELECT Path, Time, Value, Timestamp FROM %s %s %s FORMAT RowBinary`,
		pointsTable, preWhere.PreWhereSQL(), w.SQL(),
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

	ss, err := makeSeriesSet(data, aliases, rollupRules, labeler)
	if err != nil {
		return nil, nil, err
	}

	return ss, nil, nil
}
