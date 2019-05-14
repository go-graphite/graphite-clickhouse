package finder

import (
	"context"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"

	"github.com/lomik/graphite-clickhouse/config"
)

type Result interface {
	List() [][]byte
	Series() [][]byte
	Abs([]byte) []byte
}

type Finder interface {
	Result
	Execute(ctx context.Context, query string, from int64, until int64) error
}

func Find(config *config.Config, ctx context.Context, query string, from int64, until int64) (Result, error) {
	opts := clickhouse.Options{
		Timeout:        config.ClickHouse.TreeTimeout.Value(),
		ConnectTimeout: config.ClickHouse.ConnectTimeout.Value(),
	}

	fnd := func() Finder {
		var f Finder

		if config.ClickHouse.TaggedTable != "" && strings.HasPrefix(strings.TrimSpace(query), "seriesByTag") {
			f = NewTagged(config.ClickHouse.Url, config.ClickHouse.TaggedTable, opts)

			if len(config.Common.Blacklist) > 0 {
				f = WrapBlacklist(f, config.Common.Blacklist)
			}

			return f
		}

		if config.ClickHouse.IndexTable != "" {
			f = NewIndex(
				config.ClickHouse.Url,
				config.ClickHouse.IndexTable,
				config.ClickHouse.IndexUseDaily,
				clickhouse.Options{
					Timeout:        config.ClickHouse.IndexTimeout.Value(),
					ConnectTimeout: config.ClickHouse.ConnectTimeout.Value(),
				},
			)
		} else {
			if from > 0 && until > 0 && config.ClickHouse.DateTreeTable != "" {
				f = NewDateFinder(config.ClickHouse.Url, config.ClickHouse.DateTreeTable, config.ClickHouse.DateTreeTableVersion, opts)
			} else {
				f = NewBase(config.ClickHouse.Url, config.ClickHouse.TreeTable, opts)
			}

			if config.ClickHouse.ReverseTreeTable != "" {
				f = WrapReverse(f, config.ClickHouse.Url, config.ClickHouse.ReverseTreeTable, opts)
			}
		}

		if config.ClickHouse.TagTable != "" {
			f = WrapTag(f, config.ClickHouse.Url, config.ClickHouse.TagTable, opts)
		}

		if config.ClickHouse.ExtraPrefix != "" {
			f = WrapPrefix(f, config.ClickHouse.ExtraPrefix)
		}

		if len(config.Common.Blacklist) > 0 {
			f = WrapBlacklist(f, config.Common.Blacklist)
		}

		return f

	}()

	err := fnd.Execute(ctx, query, from, until)
	if err != nil {
		return nil, err
	}

	return fnd.(Result), err
}

// Leaf strips last dot and detect IsLeaf
func Leaf(value []byte) ([]byte, bool) {
	if len(value) > 0 && value[len(value)-1] == '.' {
		return value[:len(value)-1], false
	}

	return value, true
}
