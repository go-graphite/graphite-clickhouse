package finder

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"github.com/msaf1980/go-stringutils"
)

type TagCountQuerier struct {
	url                  string
	table                string
	opts                 clickhouse.Options
	useCarbonBehavior    bool
	dontMatchMissingTags bool
	dailyEnabled         bool
	body                 []byte
	stats                []metrics.FinderStat
}

func NewTagCountQuerier(url, table string, opts clickhouse.Options, useCarbonBehavior, dontMatchMissingTags, dailyEnabled bool) *TagCountQuerier {
	return &TagCountQuerier{
		url:                  url,
		table:                table,
		opts:                 opts,
		useCarbonBehavior:    useCarbonBehavior,
		dontMatchMissingTags: dontMatchMissingTags,
		dailyEnabled:         dailyEnabled,
	}
}

func (tcq *TagCountQuerier) GetCostsFromCountTable(ctx context.Context, terms []TaggedTerm, from int64, until int64) (map[string]*config.Costs, error) {
	w := where.New()
	eqTermCount := 0

	for i := 0; i < len(terms); i++ {
		if terms[i].Op == TaggedTermEq && !terms[i].HasWildcard && terms[i].Value != "" {
			sqlTerm, err := TaggedTermWhere1(&terms[i], tcq.useCarbonBehavior, tcq.dontMatchMissingTags)
			if err != nil {
				return nil, err
			}

			w.Or(sqlTerm)

			eqTermCount++
		}
	}

	if w.SQL() == "" {
		return nil, nil
	}

	if tcq.dailyEnabled {
		w.Andf(
			"Date >= '%s' AND Date <= '%s'",
			date.FromTimestampToDaysFormat(from),
			date.UntilTimestampToDaysFormat(until),
		)
	} else {
		w.Andf(
			"Date >= '%s'",
			date.FromTimestampToDaysFormat(from),
		)
	}

	sql := fmt.Sprintf("SELECT Tag1, sum(Count) as cnt FROM %s %s GROUP BY Tag1 FORMAT TabSeparatedRaw", tcq.table, w.SQL())

	var err error

	tcq.stats = append(tcq.stats, metrics.FinderStat{})
	stat := &tcq.stats[len(tcq.stats)-1]
	stat.Table = tcq.table

	tcq.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(scope.WithTable(ctx, tcq.table), tcq.url, sql, tcq.opts, nil)
	if err != nil {
		return nil, err
	}

	rows := tcq.List()

	// create cost var to validate CH response without writing to t.taggedCosts
	var costs map[string]*config.Costs

	costs, err = chResultToCosts(rows)
	if err != nil {
		return nil, err
	}

	// The metric does not exist if the response has less rows
	// than there were tags with '=' op in the initial request
	// This is due to each tag-value pair of a metric being written
	// exactly one time as Tag1
	if len(rows) < eqTermCount {
		tcq.body = []byte{}
		return nil, nil
	}

	return costs, nil
}

func chResultToCosts(body [][]byte) (map[string]*config.Costs, error) {
	costs := make(map[string]*config.Costs, 0)

	for i := 0; i < len(body); i++ {
		s := stringutils.UnsafeString(body[i])

		tag, val, count, err := parseTag1CountRow(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse result from clickhouse while querying for tag costs: %s", err.Error())
		}

		if costs[tag] == nil {
			costs[tag] = &config.Costs{Cost: nil, ValuesCost: make(map[string]int, 0)}
		}

		costs[tag].ValuesCost[val] = count
	}

	return costs, nil
}

func parseTag1CountRow(s string) (string, string, int, error) {
	var (
		tag1, count, tag, val string
		cnt, n                int
		err                   error
	)

	if tag1, count, n = stringutils.Split2(s, "\t"); n != 2 {
		return "", "", 0, fmt.Errorf("no tag count")
	}

	if tag, val, n = stringutils.Split2(tag1, "="); n != 2 {
		return "", "", 0, fmt.Errorf("no '=' in Tag1")
	}

	if cnt, err = strconv.Atoi(count); err != nil {
		return "", "", 0, fmt.Errorf("can't convert count to int")
	}

	return tag, val, cnt, nil
}

func (t *TagCountQuerier) List() [][]byte {
	if t.body == nil {
		return [][]byte{}
	}

	rows := bytes.Split(t.body, []byte{'\n'})

	skip := 0

	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}

		if skip > 0 {
			rows[i-skip] = rows[i]
		}
	}

	rows = rows[:len(rows)-skip]

	return rows
}

func (tcq *TagCountQuerier) Stats() []metrics.FinderStat {
	return tcq.stats
}
