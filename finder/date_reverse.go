package finder

import (
	"context"
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type DateFinderV3 struct {
	*BaseFinder
}

// Same as v2, but reversed
func NewDateFinderV3(url string, table string, timeout time.Duration) Finder {
	b := &BaseFinder{
		url:     url,
		table:   table,
		timeout: timeout,
	}

	return &DateFinderV3{b}
}

func (f *DateFinderV3) Execute(ctx context.Context, query string, from int64, until int64) (err error) {
	where := f.where(ReverseString(query))

	dateWhere := NewWhere()
	dateWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(from, 0).Format("2006-01-02"),
		time.Unix(until, 0).Format("2006-01-02"),
	)

	f.body, err = clickhouse.Query(
		ctx,
		f.url,
		fmt.Sprintf(
			`SELECT Path FROM %s WHERE (%s) AND (%s) GROUP BY Path HAVING argMax(Deleted, Version)==0`,
			f.table, dateWhere.String(), where),
		f.table,
		f.timeout,
	)

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
