package finder

import (
	"context"
	"strings"
	"time"
)

type ReverseFinder struct {
	wrapped Finder
	ctx     context.Context // for clickhouse.Query
	url     string          // clickhouse dsn
	table   string          // graphite_reverse_tree table
	timeout time.Duration   // clickhouse query timeout
}

func Reverse(target string) string {
	a := strings.Split(target, ".")
	l := len(a)
	for i := 0; i < l/2; i++ {
		a[i], a[l-i-1] = a[l-i-1], a[i]
	}

	return strings.Join(a, ".")
}

func WrapReverse(f Finder, ctx context.Context, url string, table string, timeout time.Duration) *ReverseFinder {
	return &ReverseFinder{
		wrapped: f,
		ctx:     ctx,
		url:     url,
		table:   table,
		timeout: timeout,
	}
}
