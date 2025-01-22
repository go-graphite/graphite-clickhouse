package finder

import (
	"context"
	"strings"
	"sync"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type singleFinderInfo struct {
	err  error
	f    Finder
	stat FinderStat
}

// SplitPlainQueryFinder will try to split queries like {first,second}.some.metric into n queries (n - number of cases inside {}).
// No matter if '{}' in first node or not.
type SplitPlainQueryFinder struct {
	// useCache flag for internal finders.
	useCache bool
	// wrapped finder will be called if we can't split query.
	wrapped Finder
	// useWrapped indicated if we should use wrapped Finder.
	useWrapped bool
	// wildcardFinders will be used for queries (got after split) which have wildcard.
	wildcardFinders []singleFinderInfo
}

// WrapSplit wraps given finder with SplitPlainQueryFinder logic.
func WrapSplit(f Finder, useCache bool) *SplitPlainQueryFinder {
	return &SplitPlainQueryFinder{
		useCache:   useCache,
		wrapped:    f,
		useWrapped: false,
	}
}

func (splitFinder *SplitPlainQueryFinder) Execute(
	ctx context.Context,
	config *config.Config,
	query string,
	from int64,
	until int64,
	stat *FinderStat,
) error {
	idx := strings.IndexAny(query, "{}")
	if idx == -1 {
		splitFinder.useWrapped = true
		return splitFinder.wrapped.Execute(ctx, config, query, from, until, stat)
	}

	splitQueries, err := splitQuery(query)
	if err != nil {
		return err
	}

	queriesWithWildcards := make([]string, 0)
	queriesNoWildcards := make([]string, 0)
	for _, q := range splitQueries {
		if where.HasWildcard(q) {
			queriesWithWildcards = append(queriesWithWildcards, q)
		} else {
			queriesNoWildcards = append(queriesNoWildcards, q)
		}
	}

	splitFinder.prepareWildcardFinders(ctx, config, query, from, until, len(queriesWithWildcards))

	wg := sync.WaitGroup{}
	wg.Add(len(splitFinder.wildcardFinders))
	for i := range splitFinder.wildcardFinders {
		go splitFinder.executeSingleFinder(
			&wg,
			&splitFinder.wildcardFinders[i],
			ctx,
			config,
			queriesWithWildcards[i],
			from,
			until,
		)
	}

	// TODO: execute non-wildcards queries in single SQL-query with multiple values in `WHERE Path IN (...)` condition.

	return nil
}

func splitQuery(query string) ([]string, error) {
	splitQueries := make([]string, 0)
	err := where.GlobExpandSimple(query, "", &splitQueries)
	if err != nil {
		return nil, err
	}

	return splitQueries, nil
}

func (splitFinder *SplitPlainQueryFinder) prepareWildcardFinders(
	ctx context.Context,
	config *config.Config,
	query string,
	from int64,
	until int64,
	count int,
) {
	if count == 0 {
		return
	}

	splitFinder.wildcardFinders = make([]singleFinderInfo, 0, count)
	for i := 0; i < count; i++ {
		splitFinder.wildcardFinders = append(
			splitFinder.wildcardFinders,
			singleFinderInfo{
				err: nil,
				f:   newPlainFinder(ctx, config, query, from, until, splitFinder.useCache),
			},
		)
	}
}

func (splitFinder *SplitPlainQueryFinder) executeSingleFinder(
	wg *sync.WaitGroup,
	finderInfo *singleFinderInfo,
	ctx context.Context,
	config *config.Config,
	query string,
	from int64,
	until int64,
) {
	defer wg.Done()

	finderInfo.err = finderInfo.f.Execute(ctx, config, query, from, until, &finderInfo.stat)
}

func (splitFinder *SplitPlainQueryFinder) List() [][]byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.List()
	}

	return EmptyList
}

func (splitFinder *SplitPlainQueryFinder) Series() [][]byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Series()
	}

	return nil
}

func (splitFinder *SplitPlainQueryFinder) Abs(v []byte) []byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Abs(v)
	}

	return nil
}

func (splitFinder *SplitPlainQueryFinder) Bytes() ([]byte, error) {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Bytes()
	}

	return nil, ErrNotImplemented
}
