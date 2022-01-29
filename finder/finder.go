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

func newPlainFinder(ctx context.Context, config *config.Config, query string, from int64, until int64) Finder {
	opts := clickhouse.Options{
		Timeout:        config.ClickHouse.IndexTimeout,
		ConnectTimeout: config.ClickHouse.ConnectTimeout,
	}

	var f Finder

	if config.ClickHouse.TaggedTable != "" && strings.HasPrefix(strings.TrimSpace(query), "seriesByTag") {
		f = NewTagged(config.ClickHouse.URL, config.ClickHouse.TaggedTable, false, opts, config.ClickHouse.TaggedCosts)

		if len(config.Common.Blacklist) > 0 {
			f = WrapBlacklist(f, config.Common.Blacklist)
		}

		return f
	}

	if config.ClickHouse.IndexTable != "" {
		f = NewIndex(
			config.ClickHouse.URL,
			config.ClickHouse.IndexTable,
			config.ClickHouse.IndexUseDaily,
			config.ClickHouse.IndexReverse,
			config.ClickHouse.IndexReverses,
			clickhouse.Options{
				Timeout:        config.ClickHouse.IndexTimeout,
				ConnectTimeout: config.ClickHouse.ConnectTimeout,
			},
		)
	} else {
		if from > 0 && until > 0 && config.ClickHouse.DateTreeTable != "" {
			f = NewDateFinder(config.ClickHouse.URL, config.ClickHouse.DateTreeTable, config.ClickHouse.DateTreeTableVersion, opts)
		} else {
			f = NewBase(config.ClickHouse.URL, config.ClickHouse.TreeTable, opts)
		}

		if config.ClickHouse.ReverseTreeTable != "" {
			f = WrapReverse(f, config.ClickHouse.URL, config.ClickHouse.ReverseTreeTable, opts)
		}
	}

	if config.ClickHouse.TagTable != "" {
		f = WrapTag(f, config.ClickHouse.URL, config.ClickHouse.TagTable, opts)
	}

	if config.ClickHouse.ExtraPrefix != "" {
		f = WrapPrefix(f, config.ClickHouse.ExtraPrefix)
	}

	if len(config.Common.Blacklist) > 0 {
		f = WrapBlacklist(f, config.Common.Blacklist)
	}

	return f
}

func Find(config *config.Config, ctx context.Context, query string, from int64, until int64) (Result, error) {
	fnd := newPlainFinder(ctx, config, query, from, until)
	err := fnd.Execute(ctx, query, from, until)
	if err != nil {
		return nil, err
	}

	return fnd.(Result), nil
}

// Leaf strips last dot and detect IsLeaf
func Leaf(value []byte) ([]byte, bool) {
	if len(value) > 0 && value[len(value)-1] == '.' {
		return value[:len(value)-1], false
	}

	return value, true
}

func FindTagged(config *config.Config, ctx context.Context, terms []TaggedTerm, from int64, until int64) (Result, error) {
	opts := clickhouse.Options{
		Timeout:        config.ClickHouse.IndexTimeout,
		ConnectTimeout: config.ClickHouse.ConnectTimeout,
	}

	plain := makePlainFromTagged(terms)
	if plain != nil {
		plain.wrappedPlain = newPlainFinder(ctx, config, plain.Target(), from, until)
		err := plain.Execute(ctx, plain.Target(), from, until)
		if err != nil {
			return nil, err
		}
		return Result(plain), nil
	}

	fnd := NewTagged(config.ClickHouse.URL, config.ClickHouse.TaggedTable, true, opts, config.ClickHouse.TaggedCosts)

	err := fnd.ExecutePrepared(ctx, terms, from, until)
	if err != nil {
		return nil, err
	}

	return Result(fnd), nil
}
