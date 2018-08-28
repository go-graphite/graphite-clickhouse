package finder

import (
	"context"
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
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

func (b *DateFinder) Execute(ctx context.Context, query string, from int64, until int64) (err error) {
	where := b.where(query)
	where.And("Deleted = 0")

	dateWhere := NewWhere()
	dateWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(from, 0).Format("2006-01-02"),
		time.Unix(until, 0).Format("2006-01-02"),
	)

	if b.tableVersion == 2 {
		b.body, err = clickhouse.Query(
			ctx,
			b.url,
			fmt.Sprintf(
				`SELECT Path FROM %s PREWHERE (%s) WHERE %s GROUP BY Path`,
				b.table, dateWhere.String(), where.String()),
			b.table,
			b.opts,
		)
	} else {
		b.body, err = clickhouse.Query(
			ctx,
			b.url,
			fmt.Sprintf(`SELECT DISTINCT Path FROM %s PREWHERE (%s) WHERE (%s)`, b.table, dateWhere.String(), where),
			b.table,
			b.opts,
		)
	}

	return
}
