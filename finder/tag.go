package finder

import (
	"context"
	"strings"
	"time"
)

type TagFinder struct {
	wrapped Finder
	ctx     context.Context // for clickhouse.Query
	url     string          // clickhouse dsn
	table   string          // graphite_tag table
	timeout time.Duration   // clickhouse query timeout
}

func WrapTag(f Finder, ctx context.Context, url string, table string, timeout time.Duration) Finder {
	return &TagFinder{
		wrapped: f,
		ctx:     ctx,
		url:     url,
		table:   table,
		timeout: timeout,
	}
}

func (t *TagFinder) Execute(query string) error {
	qs := strings.Split(query, ".")
	if len(qs) == 0 {
		return nil
	}

	if qs[0] != "_tag" {
		return t.wrapped.Execute(query)
	}

	return nil
}

func (t *TagFinder) List() [][]byte {
	return nil
}

func (t *TagFinder) Series() [][]byte {
	return nil
}

func (t *TagFinder) Abs([]byte) []byte {
	return nil
}
