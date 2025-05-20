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
	Bytes() ([]byte, error)
}

type FinderStat struct {
	ReadBytes   int64
	ChReadRows  int64
	ChReadBytes int64
	Table       string
}

type Finder interface {
	Result
	Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) error
}

func newPlainFinder(ctx context.Context, config *config.Config, query string, from int64, until int64, useCache bool) Finder {
	opts := clickhouse.Options{
		TLSConfig:      config.ClickHouse.TLSConfig,
		Timeout:        config.ClickHouse.IndexTimeout,
		ConnectTimeout: config.ClickHouse.ConnectTimeout,
	}

	var f Finder

	if config.ClickHouse.TaggedTable != "" && strings.HasPrefix(strings.TrimSpace(query), "seriesByTag") {
		f = NewTagged(
			config.ClickHouse.URL,
			config.ClickHouse.TaggedTable,
			config.ClickHouse.TagsCountTable,
			config.ClickHouse.TaggedUseDaily,
			config.FeatureFlags.UseCarbonBehavior,
			config.FeatureFlags.DontMatchMissingTags,
			false,
			opts,
			config.ClickHouse.TaggedCosts,
		)

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
			opts,
			useCache,
		)

		if config.ClickHouse.TrySplitQuery {
			f = WrapSplitIndex(
				f,
				config.ClickHouse.WildcardMinDistance,
				config.ClickHouse.URL,
				config.ClickHouse.IndexTable,
				config.ClickHouse.IndexUseDaily,
				config.ClickHouse.IndexReverse,
				config.ClickHouse.IndexReverses,
				opts,
				useCache,
			)
		}
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

func Find(config *config.Config, ctx context.Context, query string, from int64, until int64, stat *FinderStat) (Result, error) {
	fnd := newPlainFinder(ctx, config, query, from, until, config.Common.FindCache != nil)
	err := fnd.Execute(ctx, config, query, from, until, stat)
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

func FindTagged(ctx context.Context, config *config.Config, terms []TaggedTerm, from int64, until int64, stat *FinderStat) (Result, error) {
	opts := clickhouse.Options{
		Timeout:        config.ClickHouse.IndexTimeout,
		ConnectTimeout: config.ClickHouse.ConnectTimeout,
		TLSConfig:      config.ClickHouse.TLSConfig,
	}

	useCache := config.Common.FindCache != nil

	plain := makePlainFromTagged(terms)
	if plain != nil {
		plain.wrappedPlain = newPlainFinder(ctx, config, plain.Target(), from, until, useCache)
		err := plain.Execute(ctx, config, plain.Target(), from, until, stat)
		if err != nil {
			return nil, err
		}
		return Result(plain), nil
	}

	fnd := NewTagged(
		config.ClickHouse.URL,
		config.ClickHouse.TaggedTable,
		config.ClickHouse.TagsCountTable,
		config.ClickHouse.TaggedUseDaily,
		config.FeatureFlags.UseCarbonBehavior,
		config.FeatureFlags.DontMatchMissingTags,
		true,
		opts,
		config.ClickHouse.TaggedCosts,
	)

	err := fnd.ExecutePrepared(ctx, terms, from, until, stat)
	if err != nil {
		return nil, err
	}

	return Result(fnd), nil
}
