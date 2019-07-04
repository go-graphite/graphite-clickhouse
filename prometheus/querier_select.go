package prometheus

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

func (q *Querier) lookup(from, until time.Time, labelsMatcher ...*labels.Matcher) ([]string, error) {
	matchWhere, err := wherePromQL(labelsMatcher)
	if err != nil {
		return nil, err
	}

	where := finder.NewWhere()
	where.Andf(
		"Date >='%s' AND Date <= '%s'",
		from.Format("2006-01-02"),
		until.Format("2006-01-02"),
	)
	where.And(matchWhere)

	sql := fmt.Sprintf(
		"SELECT Path FROM %s WHERE %s GROUP BY Path",
		q.config.ClickHouse.TaggedTable,
		where.String(),
	)
	body, err := clickhouse.Query(
		q.ctx,
		q.config.ClickHouse.Url,
		sql,
		q.config.ClickHouse.TaggedTable,
		clickhouse.Options{
			Timeout:        q.config.ClickHouse.IndexTimeout.Value(),
			ConnectTimeout: q.config.ClickHouse.ConnectTimeout.Value(),
		},
	)

	if err != nil {
		return nil, err
	}

	result := strings.Split(string(body), "\n")
	rm := 0
	for i := 0; i < len(result); i++ {
		if result[i] == "" {
			rm++
			continue
		}
		if rm > 0 {
			result[i-rm] = result[i]
		}
	}

	return result[:len(result)-rm], nil
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

	metrics, err := q.lookup(from, until, labelsMatcher...)
	if err != nil {
		return nil, nil, err
	}

	if len(metrics) == 0 {
		ss, _ := makeSeriesSet(nil, nil)
		return ss, nil, nil
	}

	if selectParams == nil {
		// /api/v1/series?match[]=...
		return newMetricsSet(metrics), nil, nil
	}

	listBuf := new(bytes.Buffer)
	count := 0
	for _, m := range metrics {
		if count > 0 {
			listBuf.WriteByte(',')
		}
		listBuf.WriteString(finder.Q(m))
		count++
	}

	preWhere := finder.NewWhere()
	preWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		from.Format("2006-01-02"),
		until.Format("2006-01-02"),
	)

	where := finder.NewWhere()
	if count > 1 {
		where.Andf("Path in (%s)", listBuf.String())
	} else {
		where.Andf("Path = %s", listBuf.String())
	}

	where.Andf("Time >= %d AND Time <= %d", from.Unix(), until.Unix()+1)

	pointsTable, _, rollupRules := render.SelectDataTable(q.config, from.Unix(), until.Unix(), []string{})
	if pointsTable == "" {
		return nil, nil, fmt.Errorf("data table is not specified")
	}

	query := fmt.Sprintf(
		`
		SELECT
			Path, Time, Value, Timestamp
		FROM %s
		PREWHERE (%s)
		WHERE (%s)
		FORMAT RowBinary
		`,
		pointsTable,
		preWhere.String(),
		where.String(),
	)

	body, err := clickhouse.Reader(
		q.ctx,
		q.config.ClickHouse.Url,
		query,
		pointsTable,
		clickhouse.Options{Timeout: q.config.ClickHouse.DataTimeout.Value(), ConnectTimeout: q.config.ClickHouse.ConnectTimeout.Value()},
	)

	if err != nil {
		return nil, nil, err
	}

	data, err := render.DataParse(body, nil, false)
	if err != nil {
		return nil, nil, err
	}

	data.Points.Sort()
	data.Points.Uniq()

	if data.Points.Len() == 0 {
		return nil, nil, nil
	}

	ss, err := makeSeriesSet(data, rollupRules)
	if err != nil {
		return nil, nil, err
	}

	return ss, nil, nil
}
