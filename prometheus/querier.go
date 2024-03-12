//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

// Querier provides reading access to time series data.
type Querier struct {
	config *config.Config
	mint   int64
	maxt   int64
	ctx    context.Context
}

// Close releases the resources of the Querier.
func (q *Querier) Close() error {
	return nil
}

// LabelValues returns all potential values for a label name.
func (q *Querier) LabelValues(label string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	// @TODO: support matchers
	terms := []finder.TaggedTerm{
		{
			Key:         label,
			Op:          finder.TaggedTermEq,
			Value:       "*",
			HasWildcard: true,
		},
	}

	matcherTerms, err := makeTaggedFromPromQL(matchers)
	if err != nil {
		return nil, nil, err
	}
	terms = append(terms, matcherTerms...)
	
	wr, _, err := finder.TaggedWhere(terms)
	if err != nil {
		return nil, nil, err
	}
	//wr.And(where.HasPrefix("Tag1", label+"="))

	fromDate := timeNow().AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays)
	wr.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	sql := fmt.Sprintf("SELECT splitByChar('=', Tag1)[2] as value FROM %s %s GROUP BY value ORDER BY value",
		q.config.ClickHouse.TaggedTable,
		wr.SQL(),
	)

	body, _, _, err := clickhouse.Query(
		scope.WithTable(q.ctx, q.config.ClickHouse.TaggedTable),
		q.config.ClickHouse.URL,
		sql,
		clickhouse.Options{
			TLSConfig:      q.config.ClickHouse.TLSConfig,
			Timeout:        q.config.ClickHouse.IndexTimeout,
			ConnectTimeout: q.config.ClickHouse.ConnectTimeout,
		},
		nil,
	)
	if err != nil {
		return nil, nil, err
	}

	rows := strings.Split(string(body), "\n")
	if len(rows) > 0 && rows[len(rows)-1] == "" {
		rows = rows[:len(rows)-1]
	}

	return rows, nil, nil
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *Querier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	// @TODO support matchers
	terms, err := makeTaggedFromPromQL(matchers)
	if err != nil {
		return nil, nil, err
	}
	w := where.New()
	for i := 0; i < len(terms); i++ {
		and, err := finder.TaggedTermWhereN(&terms[i])
		if err != nil {
			return nil, nil, err
		}
		w.And(and)
	}
	fromDate := time.Now().AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays).UTC()
	w.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	sql := fmt.Sprintf("SELECT splitByChar('=', Tag1)[1] as value FROM %s %s GROUP BY value ORDER BY value",
		q.config.ClickHouse.TaggedTable,
		w.SQL(),
	)

	body, _, _, err := clickhouse.Query(
		scope.WithTable(q.ctx, q.config.ClickHouse.TaggedTable),
		q.config.ClickHouse.URL,
		sql,
		clickhouse.Options{
			Timeout:        q.config.ClickHouse.IndexTimeout,
			ConnectTimeout: q.config.ClickHouse.ConnectTimeout,
			TLSConfig:      q.config.ClickHouse.TLSConfig,
		},
		nil,
	)
	if err != nil {
		return nil, nil, err
	}

	rows := strings.Split(string(body), "\n")
	if len(rows) > 0 && rows[len(rows)-1] == "" {
		rows = rows[:len(rows)-1]
	}

	return rows, nil, nil
}
