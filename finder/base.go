package finder

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type BaseFinder struct {
	ctx     context.Context // for clickhouse.Query
	url     string          // clickhouse dsn
	table   string          // graphite_tree table
	timeout time.Duration   // clickhouse query timeout
}

func NewBase(ctx context.Context, url string, table string, timeout time.Duration) Finder {
	return &BaseFinder{
		ctx:     ctx,
		url:     url,
		table:   table,
		timeout: timeout,
	}
}

func (b *BaseFinder) where(query string) (where string) {
	level := strings.Count(query, ".") + 1

	and := func(exp string) {
		if exp == "" {
			return
		}
		if where != "" {
			where = fmt.Sprintf("%s AND (%s)", where, exp)
		} else {
			where = fmt.Sprintf("(%s)", exp)
		}
	}

	and(fmt.Sprintf("Level = %d", level))

	if query == "*" {
		return
	}

	// simple metric
	if !HasWildcard(query) {
		and(fmt.Sprintf("Path = %s OR Path = %s", Q(query), Q(query+".")))
		return
	}

	// before any wildcard symbol
	simplePrefix := query[:strings.IndexAny(query, "[]{}*")]

	if len(simplePrefix) > 0 {
		and(fmt.Sprintf("Path LIKE %s", Q(simplePrefix+`%`)))
	}

	// prefix search like "metric.name.xx*"
	if len(simplePrefix) == len(query)-1 && query[len(query)-1] == '*' {
		return
	}

	and(fmt.Sprintf("match(Path, %s)", Q(`^`+GlobToRegexp(query)+`$`)))
	return
}

func (b *BaseFinder) Execute(query string) error {
	fmt.Println("execute", query)
	return nil
}

func (b *BaseFinder) List() [][]byte {
	return nil
}

func (b *BaseFinder) Series() [][]byte {
	return nil
}

func (b *BaseFinder) Abs([]byte) []byte {
	// @TODO
	return nil
}
