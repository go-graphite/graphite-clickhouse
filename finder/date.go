package finder

import (
	"context"
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type DateFinder struct {
	*BaseFinder
	tableVersion int
}

func NewDateFinder(url string, table string, tableVersion int, opts clickhouse.Options) Finder {
	if tableVersion == 3 {
		return NewDateFinderV3(url, table, opts)
	}

	b := &BaseFinder{
		url:   url,
		table: table,
		opts:  opts,
	}

	return &DateFinder{b, tableVersion}
}

func (b *DateFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	w := b.where(query)

	dateWhere := where.New()
	dateWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(from, 0).Format("2006-01-02"),
		time.Unix(until, 0).Format("2006-01-02"),
	)

	if b.tableVersion == 2 {
		b.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(
			scope.WithTable(ctx, b.table),
			b.url,
			// TODO: consider consistent query generator
			fmt.Sprintf(`SELECT Path FROM %s PREWHERE (%s) WHERE %s GROUP BY Path FORMAT TabSeparatedRaw`, b.table, dateWhere, w),
			b.opts,
			nil,
		)
	} else {
		b.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(
			scope.WithTable(ctx, b.table),
			b.url,
			// TODO: consider consistent query generator
			fmt.Sprintf(`SELECT DISTINCT Path FROM %s PREWHERE (%s) WHERE (%s) FORMAT TabSeparatedRaw`, b.table, dateWhere, w),
			b.opts,
			nil,
		)
	}
	stat.ReadBytes = int64(len(b.body))
	stat.Table = b.table

	return
}
