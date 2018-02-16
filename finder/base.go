package finder

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type BaseFinder struct {
	url     string        // clickhouse dsn
	table   string        // graphite_tree table
	timeout time.Duration // clickhouse query timeout
	body    []byte        // clickhouse response body
}

func NewBase(url string, table string, timeout time.Duration) Finder {
	return &BaseFinder{
		url:     url,
		table:   table,
		timeout: timeout,
	}
}

func (b *BaseFinder) where(query string) string {
	level := strings.Count(query, ".") + 1

	w := NewWhere()

	w.Andf("Level = %d", level)

	if query == "*" {
		return w.String()
	}

	// simple metric
	if !HasWildcard(query) {
		w.Andf("Path = %s OR Path = %s", Q(query), Q(query+"."))
		return w.String()
	}

	// before any wildcard symbol
	simplePrefix := query[:strings.IndexAny(query, "[]{}*")]

	if len(simplePrefix) > 0 {
		w.Andf("Path LIKE %s", Q(simplePrefix+`%`))
	}

	// prefix search like "metric.name.xx*"
	if len(simplePrefix) == len(query)-1 && query[len(query)-1] == '*' {
		return w.String()
	}

	// Q() replaces \ with \\, so using \. does not work here.
	// work around with [.]
	w.Andf("match(Path, %s)", Q(`^`+GlobToRegexp(query)+`[.]?$`))
	return w.String()
}

func (b *BaseFinder) Execute(ctx context.Context, query string, from int64, until int64) (err error) {
	where := b.where(query)

	b.body, err = clickhouse.Query(
		ctx,
		b.url,
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path HAVING argMax(Deleted, Version)==0", b.table, where),
		b.timeout,
	)

	return
}

func (b *BaseFinder) makeList(onlySeries bool) [][]byte {
	if b.body == nil {
		return [][]byte{}
	}

	rows := bytes.Split(b.body, []byte{'\n'})

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}
		if onlySeries && rows[i][len(rows[i])-1] == '.' {
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

func (b *BaseFinder) List() [][]byte {
	return b.makeList(false)
}

func (b *BaseFinder) Series() [][]byte {
	return b.makeList(true)
}

func (b *BaseFinder) Abs(v []byte) []byte {
	return v
}
