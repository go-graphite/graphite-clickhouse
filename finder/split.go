package finder

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/errs"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type indexFinderParams struct {
	url          string
	table        string
	opts         clickhouse.Options
	dailyEnabled bool
	useCache     bool
	reverse      string
	confReverses config.IndexReverses
}

// SplitIndexFinder will try to split queries like {first,second}.some.metric into n queries (n - number of cases inside {}).
// No matter if '{}' in first node or not. Only one {} will be split.
type SplitIndexFinder struct {
	indexFinderParams
	// wrapped finder will be called if we can't split query.
	wrapped Finder
	body    []byte
	rows    [][]byte
	stats   []metrics.FinderStat
	// useWrapped indicated if we should use wrapped Finder.
	useWrapped          bool
	useReverse          bool
	wildcardMinDistance int
}

// WrapSplitIndex wraps given finder with SplitIndexFinder logic.
func WrapSplitIndex(
	f Finder,
	wildcardMinDistance int,
	url string,
	table string,
	dailyEnabled bool,
	reverse string,
	reverses config.IndexReverses,
	opts clickhouse.Options,
	useCache bool,
) *SplitIndexFinder {
	return &SplitIndexFinder{
		wrapped:             f,
		useWrapped:          false,
		useReverse:          false,
		wildcardMinDistance: wildcardMinDistance,
		indexFinderParams: indexFinderParams{
			url:          url,
			table:        table,
			dailyEnabled: dailyEnabled,
			reverse:      reverse,
			confReverses: reverses,
			opts:         opts,
			useCache:     useCache,
		},
	}
}

// Execute will try to split query if it contains list in it. If query can't be split wrapped Finder will be used.
// Use List, Series or Bytes after calling Execute to get data.
func (splitFinder *SplitIndexFinder) Execute(
	ctx context.Context,
	config *config.Config,
	query string,
	from int64,
	until int64,
) error {
	if where.HasUnmatchedBrackets(query) {
		return errs.NewErrorWithCode("query has unmatched brackets", http.StatusBadRequest)
	}

	query = where.ClearGlob(query)

	idx := strings.IndexAny(query, "{}")
	if idx == -1 {
		splitFinder.useWrapped = true
		return splitFinder.wrapped.Execute(ctx, config, query, from, until)
	}

	splitQueries, err := splitQuery(query, config.ClickHouse.MaxNodeToSplitIndex)
	if err != nil {
		return err
	}

	if len(splitQueries) <= 1 {
		splitFinder.useWrapped = true
		return splitFinder.wrapped.Execute(ctx, config, query, from, until)
	}

	w, err := splitFinder.whereFilter(splitQueries, from, until)
	if err != nil {
		return err
	}

	splitFinder.stats = append(splitFinder.stats, metrics.FinderStat{})
	stat := &splitFinder.stats[len(splitFinder.stats)-1]

	splitFinder.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(
		scope.WithTable(ctx, splitFinder.table),
		splitFinder.url,
		// TODO: consider consistent query generator
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path FORMAT TabSeparatedRaw", splitFinder.table, w),
		splitFinder.opts,
		nil,
	)
	stat.Table = splitFinder.table

	if err != nil {
		return err
	}

	stat.ReadBytes = int64(len(splitFinder.body))
	splitFinder.body, splitFinder.rows, _ = splitIndexBody(splitFinder.body, splitFinder.useReverse, splitFinder.useCache)

	return nil
}

func splitQuery(query string, maxNodeToSplitIdx int) ([]string, error) {
	splitQueries := make([]string, 0, 1)

	firstClosingBracketIndex := strings.Index(query, "}")
	lastOpenBracketIndex := strings.LastIndex(query, "{")

	firstOpenBracketsIndex := strings.Index(query, "{")
	directNodeCount := strings.Count(query[:firstOpenBracketsIndex], ".")
	directWildcardIndex := where.IndexWildcard(query[:firstOpenBracketsIndex])

	lastClosingBracketIndex := strings.LastIndex(query, "}")
	reverseNodeCount := strings.Count(query[lastClosingBracketIndex:], ".")

	var reverseWildcardIndex int
	if lastClosingBracketIndex == len(query)-1 {
		reverseWildcardIndex = -1
	} else {
		reverseWildcardIndex = where.IndexLastWildcard(query[lastClosingBracketIndex+1:])
	}

	useDirect := true

	if directWildcardIndex >= 0 && reverseWildcardIndex >= 0 {
		return []string{query}, nil
	} else if directWildcardIndex < 0 && reverseWildcardIndex >= 0 {
		if directNodeCount > maxNodeToSplitIdx {
			return []string{query}, nil
		}

		useDirect = true
	} else if directWildcardIndex >= 0 && reverseWildcardIndex < 0 {
		if reverseNodeCount > maxNodeToSplitIdx {
			return []string{query}, nil
		}

		useDirect = false
	} else {
		if directNodeCount > maxNodeToSplitIdx && reverseNodeCount > maxNodeToSplitIdx {
			return []string{query}, nil
		}
	}

	if lastOpenBracketIndex < firstClosingBracketIndex {
		// we have only one bracket in query
		err := where.GlobExpandSimple(query, "", &splitQueries)
		if err != nil {
			return nil, err
		}

		return splitQueries, nil
	}

	choicesInLeftMost := strings.Count(query[firstOpenBracketsIndex:firstClosingBracketIndex], ",")
	choicesInRightMost := strings.Count(query[lastOpenBracketIndex:lastClosingBracketIndex], ",")

	if directWildcardIndex < 0 && reverseWildcardIndex < 0 {
		if directNodeCount > reverseNodeCount {
			if directNodeCount > maxNodeToSplitIdx {
				return []string{query}, nil
			}

			useDirect = true
		} else if reverseNodeCount > directNodeCount {
			if reverseNodeCount > maxNodeToSplitIdx {
				return []string{query}, nil
			}

			useDirect = false
		} else {
			if choicesInLeftMost >= choicesInRightMost {
				useDirect = true
			} else {
				useDirect = false
			}
		}
	}

	var prefix, suffix, queryPart string
	if useDirect {
		prefix = ""
		queryPart = query[:firstClosingBracketIndex+1]
		suffix = query[firstClosingBracketIndex+1:]
	} else {
		prefix = query[:lastOpenBracketIndex]
		queryPart = query[lastOpenBracketIndex:]
		suffix = ""
	}

	splitQueries, err := splitPartOfQuery(prefix, queryPart, suffix)
	if err != nil {
		return nil, err
	}

	return splitQueries, nil
}

func splitPartOfQuery(prefix, queryPart, suffix string) ([]string, error) {
	splitQueries := make([]string, 0)

	err := where.GlobExpandSimple(queryPart, "", &splitQueries)
	if err != nil {
		return nil, err
	}

	for i := range splitQueries {
		splitQueries[i] = prefix + splitQueries[i] + suffix
	}

	return splitQueries, nil
}

func (splitFinder *SplitIndexFinder) whereFilter(queries []string, from, until int64) (*where.Where, error) {
	queryWithWildcardIdx := -1

	for i, q := range queries {
		err := validatePlainQuery(q, splitFinder.wildcardMinDistance)
		if err != nil {
			return nil, err
		}

		if queryWithWildcardIdx < 0 && where.HasWildcard(q) {
			queryWithWildcardIdx = i
		}
	}

	if queryWithWildcardIdx >= 0 {
		splitFinder.useReverse = (&IndexFinder{
			confReverses: splitFinder.confReverses,
			confReverse:  config.IndexReverse[splitFinder.reverse],
		}).useReverse(queries[queryWithWildcardIdx])
	} else {
		splitFinder.useReverse = false
	}

	nonWildcardQueries := make([]string, 0)
	aggregatedWhere := where.New()

	for _, q := range queries {
		if splitFinder.useReverse {
			q = ReverseString(q)
		}

		if !where.HasWildcard(q) {
			nonWildcardQueries = append(nonWildcardQueries, q, q+".")
		} else {
			aggregatedWhere.Or(where.TreeGlob("Path", q))
		}
	}

	if len(nonWildcardQueries) > 0 {
		aggregatedWhere.Or(where.In("Path", nonWildcardQueries))
	}

	useDates := useDaily(splitFinder.dailyEnabled, from, until)
	levelOffset := calculateIndexLevelOffset(useDates, splitFinder.useReverse)
	level := strings.Count(queries[0], ".") + 1

	aggregatedWhere.And(where.Eq("Level", level+levelOffset))
	addDatesToWhere(aggregatedWhere, useDates, from, until)

	return aggregatedWhere, nil
}

// List returns clickhouse response split by delimiter.
// If there was no split, wrapped.List will be used.
func (splitFinder *SplitIndexFinder) List() [][]byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.List()
	}

	return makeList(splitFinder.rows, false)
}

// Series same as List. If there was no split, wrapped.Series will be used.
func (splitFinder *SplitIndexFinder) Series() [][]byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Series()
	}

	return makeList(splitFinder.rows, true)
}

// Abs for this implementation returns given v.
// If there was no split, wrapped.Abs will be used.
func (splitFinder *SplitIndexFinder) Abs(v []byte) []byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Abs(v)
	}

	return v
}

// Bytes returns clickhouse response bytes.
// If there was no split, wrapped.Bytes will be used.
func (splitFinder *SplitIndexFinder) Bytes() ([]byte, error) {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Bytes()
	}

	return splitFinder.body, nil
}

func (splitFinder *SplitIndexFinder) Stats() []metrics.FinderStat {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Stats()
	}

	return splitFinder.stats
}
