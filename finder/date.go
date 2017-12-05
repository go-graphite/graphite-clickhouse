package finder

import (
	"context"
	"fmt"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type DateFinder struct {
	*BaseFinder
	dateWhere string
}

func NewDateFinder(dateWhere string, ctx context.Context, config *config.Config) Finder {
	b := &BaseFinder{
		ctx:         ctx,
		url:         config.ClickHouse.Url,
		table:       config.ClickHouse.DateTreeTable,
		expandLimit: config.ClickHouse.MetricLimitWithExpand,
		timeout:     config.ClickHouse.TreeTimeout.Value(),
	}
	return &DateFinder{b, dateWhere}
}

func (b *DateFinder) Execute(query string) (err error) {
	where := b.where(query)

	// TODO add deleted field to table?
	b.body, err = clickhouse.Query(
		b.ctx,
		b.url,
		fmt.Sprintf(`
		SELECT distinct Path FROM %s PREWHERE (%s) WHERE (%s)
		`, b.table, b.dateWhere, where),
		b.timeout,
	)

	return
}
