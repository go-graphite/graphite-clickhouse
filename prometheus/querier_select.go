package prometheus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type Metric struct {
	Path string   `json:"Path"`
	Tags []string `json:"Tags"`
}

func (q *Querier) lookup(from, until time.Time, labelsMatcher ...*labels.Matcher) ([]Metric, error) {
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
		"SELECT Path, any(Tags) as Tags FROM %s WHERE %s GROUP BY Path FORMAT JSON",
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

	resp := struct {
		Data []Metric `json:"data"`
	}{}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
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

	metrics, err := q.lookup(from, until, labelsMatcher...)
	if err != nil {
		return nil, nil, err
	}

	if len(metrics) == 0 {
		panic("not implemented")
		return nil, nil, nil
	}

	listBuf := new(bytes.Buffer)
	count := 0
	for _, m := range metrics {
		if count > 0 {
			listBuf.WriteByte(',')
		}
		listBuf.WriteString(finder.Q(m.Path))
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

	pointsTable, _, _ := render.SelectDataTable(q.config, from.Unix(), until.Unix(), []string{})
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

	return &seriesSet{data: data, offset: -1}, nil, nil
}
