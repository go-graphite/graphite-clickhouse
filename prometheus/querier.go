//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"github.com/prometheus/prometheus/model/labels"
)

// Querier provides reading access to time series data.
type Querier struct {
	config *config.Config
	mint   int64
	maxt   int64
}

// Close releases the resources of the Querier.
func (q *Querier) Close() error {
	return nil
}

// LabelValues returns all potential values for a label name.
func (q *Querier) LabelValues(ctx context.Context, label string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// @TODO: support matchers
	w := where.New()
	w.And(where.HasPrefix("Tag1", label+"="))

	fromDate := timeNow().AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays)
	w.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	sql := fmt.Sprintf("SELECT splitByChar('=', Tag1)[2] as value FROM %s %s GROUP BY value ORDER BY value",
		q.config.ClickHouse.TaggedTable,
		w.SQL(),
	)

	body, _, _, err := clickhouse.Query(
		scope.WithTable(ctx, q.config.ClickHouse.TaggedTable),
		q.config.ClickHouse.URL,
		sql,
		clickhouse.Options{
			TLSConfig:      q.config.ClickHouse.TLSConfig,
			Timeout:        q.config.ClickHouse.IndexTimeout,
			ConnectTimeout: q.config.ClickHouse.ConnectTimeout,
			CheckRequestProgress: q.config.FeatureFlags.LogQueryProgress,
			ProgressSendingInterval: q.config.ClickHouse.ProgressSendingInterval,
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
func (q *Querier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// @TODO support matchers
	w := where.New()
	fromDate := time.Now().AddDate(0, 0, -q.config.ClickHouse.TaggedAutocompleDays).UTC()
	w.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	sql := fmt.Sprintf("SELECT splitByChar('=', Tag1)[1] as value FROM %s %s GROUP BY value ORDER BY value",
		q.config.ClickHouse.TaggedTable,
		w.SQL(),
	)

	body, _, _, err := clickhouse.Query(
		scope.WithTable(ctx, q.config.ClickHouse.TaggedTable),
		q.config.ClickHouse.URL,
		sql,
		clickhouse.Options{
			Timeout:        q.config.ClickHouse.IndexTimeout,
			ConnectTimeout: q.config.ClickHouse.ConnectTimeout,
			TLSConfig:      q.config.ClickHouse.TLSConfig,
			CheckRequestProgress: q.config.FeatureFlags.LogQueryProgress,
			ProgressSendingInterval: q.config.ClickHouse.ProgressSendingInterval,
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
