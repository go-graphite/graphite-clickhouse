package finder

import (
	"context"
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

func (b *BaseFinder) Execute(query string) error {
	return nil
}

func (b *BaseFinder) List() [][]byte {
	return nil
}

func (b *BaseFinder) Series() [][]byte {
	return nil
}

func (b *BaseFinder) Abs([]byte) ([]byte, bool) {
	// @TODO
	return nil, false
}
