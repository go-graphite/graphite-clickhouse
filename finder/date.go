package finder

import (
	"context"
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type DateFinder struct {
	*BaseFinder
	tableVersion   int
	fromTimestamp  int64
	untilTimestamp int64
}

func NewDateFinder(ctx context.Context, url string, table string, tableVersion int, timeout time.Duration, fromTimestamp int64, untilTimestamp int64) Finder {
	b := &BaseFinder{
		ctx:     ctx,
		url:     url,
		table:   table,
		timeout: timeout,
	}

	return &DateFinder{b, tableVersion, fromTimestamp, untilTimestamp}
}

func (b *DateFinder) Execute(query string) (err error) {
	where := b.where(query)

	dateWhere := NewWhere()
	dateWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(b.fromTimestamp, 0).Format("2006-01-02"),
		time.Unix(b.untilTimestamp, 0).Format("2006-01-02"),
	)

	if b.tableVersion == 2 {
		b.body, err = clickhouse.Query(
			b.ctx,
			b.url,
			fmt.Sprintf(
				`SELECT Path FROM %s PREWHERE (%s) WHERE (%s) GROUP BY Path HAVING argMax(Deleted, Version)==0`,
				b.table, dateWhere.String(), where),
			b.timeout,
		)
	} else {
		b.body, err = clickhouse.Query(
			b.ctx,
			b.url,
			fmt.Sprintf(`SELECT DISTINCT Path FROM %s PREWHERE (%s) WHERE (%s)`, b.table, dateWhere.String(), where),
			b.timeout,
		)
	}

	return
}
