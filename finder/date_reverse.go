package finder

import (
	"context"
	"fmt"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type DateFinderV3 struct {
	*BaseFinder
}

// Same as v2, but reversed
func NewDateFinderV3(url string, table string, opts clickhouse.Options) Finder {
	b := &BaseFinder{
		url:   url,
		table: table,
		opts:  opts,
	}

	return &DateFinderV3{b}
}

func (f *DateFinderV3) whereFilter(query string, from int64, until int64) (*where.Where, *where.Where) {
	w := f.where(ReverseString(query))

	dateWhere := where.New()
	dateWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		date.FromTimestampToDaysFormat(from),
		date.UntilTimestampToDaysFormat(until),
	)

	return w, dateWhere
}

func (f *DateFinderV3) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	w, dateWhere := f.whereFilter(query, from, until)
	f.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(
		scope.WithTable(ctx, f.table),
		f.url,
		// TODO: consider consistent query generator
		fmt.Sprintf(`SELECT Path FROM %s WHERE (%s) AND (%s) GROUP BY Path FORMAT TabSeparatedRaw`, f.table, dateWhere, w),
		f.opts,
		nil,
	)
	stat.Table = f.table
	stat.ReadBytes = int64(len(f.body))

	return
}

func (f *DateFinderV3) List() [][]byte {
	list := f.BaseFinder.List()
	for i := 0; i < len(list); i++ {
		list[i] = ReverseBytes(list[i])
	}

	return list
}

func (f *DateFinderV3) Series() [][]byte {
	list := f.BaseFinder.Series()
	for i := 0; i < len(list); i++ {
		list[i] = ReverseBytes(list[i])
	}

	return list
}
