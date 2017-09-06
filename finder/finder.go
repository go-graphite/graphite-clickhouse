package finder

import (
	"context"

	"github.com/lomik/graphite-clickhouse/config"
)

type Finder interface {
	Execute(query string) error
	List() [][]byte
	Series() [][]byte
	Abs([]byte) []byte
}

func New(ctx context.Context, config *config.Config) Finder {
	f := NewBase(ctx, config.ClickHouse.Url, config.ClickHouse.TreeTable, config.ClickHouse.TreeTimeout.Value())

	if config.ClickHouse.ReverseTreeTable != "" {
		f = WrapReverse(f, ctx, config.ClickHouse.Url, config.ClickHouse.ReverseTreeTable, config.ClickHouse.TreeTimeout.Value())
	}

	if config.ClickHouse.TagTable != "" {
		f = WrapTag(f, ctx, config.ClickHouse.Url, config.ClickHouse.TagTable, config.ClickHouse.TreeTimeout.Value())
	}

	if config.ClickHouse.ExtraPrefix != "" {
		f = WrapPrefix(f, config.ClickHouse.ExtraPrefix)
	}

	if len(config.Common.Blacklist) > 0 {
		f = WrapBlacklist(f, config.Common.Blacklist)
	}
	return f
}

// Leaf strips last dot and detect IsLeaf
func Leaf(value []byte) ([]byte, bool) {
	if len(value) > 0 && value[len(value)-1] == '.' {
		return value[:len(value)-1], false
	}

	return value, true
}
