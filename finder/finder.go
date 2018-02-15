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

func NewLimited(ctx context.Context, config *config.Config, fromTimestamp int64, untilTimestamp int64) Finder {
	var f Finder

	if fromTimestamp > 0 && untilTimestamp > 0 && config.ClickHouse.DateTreeTable != "" {
		f = NewDateFinder(ctx, config.ClickHouse.Url, config.ClickHouse.DateTreeTable, config.ClickHouse.DateTreeTableVersion, config.ClickHouse.TreeTimeout.Value(), fromTimestamp, untilTimestamp)
	} else {
		f = NewBase(ctx, config.ClickHouse.Url, config.ClickHouse.TreeTable, config.ClickHouse.TreeTimeout.Value())
	}

	if config.ClickHouse.ReverseTreeTable != "" {
		f = WrapReverse(f, ctx, config.ClickHouse.Url, config.ClickHouse.ReverseTreeTable, config.ClickHouse.TreeTimeout.Value())
	}

	if config.ClickHouse.TaggedTable != "" && fromTimestamp > 0 && untilTimestamp > 0 {
		f = WrapTagged(f, ctx, config.ClickHouse.Url, config.ClickHouse.TaggedTable, config.ClickHouse.TreeTimeout.Value(), fromTimestamp, untilTimestamp)
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

func New(ctx context.Context, config *config.Config) Finder {
	return NewLimited(ctx, config, 0, 0)
}

// Leaf strips last dot and detect IsLeaf
func Leaf(value []byte) ([]byte, bool) {
	if len(value) > 0 && value[len(value)-1] == '.' {
		return value[:len(value)-1], false
	}

	return value, true
}
