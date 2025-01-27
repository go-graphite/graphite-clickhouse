package finder

import (
	"context"
	"net/http"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/errs"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

// SplitIndexFinder will try to split queries like {first,second}.some.metric into n queries (n - number of cases inside {}).
// No matter if '{}' in first node or not. Only one {} will be split.
type SplitIndexFinder struct {
	// wrapped finder will be called if we can't split query.
	wrapped Finder
	// useWrapped indicated if we should use wrapped Finder.
	useWrapped bool
	opts       clickhouse.Options
	useCache   bool
}

// WrapSplitIndex wraps given finder with SplitIndexFinder logic.
func WrapSplitIndex(f Finder, opts clickhouse.Options, useCache bool) *SplitIndexFinder {
	return &SplitIndexFinder{
		wrapped:    f,
		useWrapped: false,
		opts:       opts,
		useCache:   useCache,
	}
}

func (splitFinder *SplitIndexFinder) Execute(
	ctx context.Context,
	config *config.Config,
	query string,
	from int64,
	until int64,
	stat *FinderStat,
) error {
	if where.HasUnmatchedBrackets(query) {
		return errs.NewErrorWithCode("query has unmatched brackets", http.StatusBadRequest)
	}

	idx := strings.IndexAny(query, "{}")
	if idx == -1 {
		splitFinder.useWrapped = true
		return splitFinder.wrapped.Execute(ctx, config, query, from, until, stat)
	}

	splitQueries, err := splitQuery(query)
	if err != nil {
		return err
	}

	aggregatedWhere := where.New()
	for _, q := range splitQueries {
		err = validatePlainQuery(q, config.ClickHouse.WildcardMinDistance)
		if err != nil {
			return err
		}

		indexFinder := NewIndex(
			config.ClickHouse.URL,
			config.ClickHouse.IndexTable,
			config.ClickHouse.IndexUseDaily,
			config.ClickHouse.IndexReverse,
			config.ClickHouse.IndexReverses,
			splitFinder.opts,
			splitFinder.useCache,
		).(*IndexFinder)

		aggregatedWhere.Or(indexFinder.whereFilter(q, from, until).String())
	}

	// TODO: think about max_query_size

	return nil
}

func splitQuery(query string) ([]string, error) {
	splitQueries := make([]string, 0, 1)

	firstClosingBracketIndex := strings.Index(query, "}")
	lastOpenBracketIndex := strings.LastIndex(query, "{")

	if lastOpenBracketIndex < firstClosingBracketIndex {
		// we have only one bracket in query
		err := where.GlobExpandSimple(query, "", &splitQueries)
		if err != nil {
			return nil, err
		}

		return splitQueries, nil
	}

	firstOpenBracketsIndex := strings.Index(query, "{")
	directNodeCount := strings.Count(query[:firstOpenBracketsIndex], ".")
	directWildcardIndex := where.IndexWildcard(query[:firstOpenBracketsIndex])
	choicesInLeftMost := strings.Count(query[firstOpenBracketsIndex:firstClosingBracketIndex], ",")
	//fmt.Printf("\ndirect:\n\tnodeCount = %v\n\twildcardIndex = %v\n\tchoices = %v\n",
	//	directNodeCount,
	//	directWildcardIndex,
	//	choicesInLeftMost)

	lastClosingBracketIndex := strings.LastIndex(query, "}")
	reverseNodeCount := strings.Count(query[lastClosingBracketIndex:], ".")
	var reversWildcardIndex int
	if lastClosingBracketIndex == len(query)-1 {
		reversWildcardIndex = -1
	} else {
		reversWildcardIndex = where.IndexLastWildcard(query[lastClosingBracketIndex+1:])
	}
	choicesInRightMost := strings.Count(query[lastOpenBracketIndex:lastClosingBracketIndex], ",")
	//fmt.Printf("\nreverse:\n\tnodeCount = %v\n\twildcardIndex = %v\n\tchoices = %v\n",
	//	reverseNodeCount,
	//	reversWildcardIndex,
	//	choicesInRightMost)

	useDirect := true
	if directWildcardIndex >= 0 && reversWildcardIndex < 0 {
		useDirect = false
	} else if directWildcardIndex < 0 && reversWildcardIndex >= 0 {
		useDirect = true
	} else if directWildcardIndex >= 0 && reversWildcardIndex >= 0 {
		if choicesInLeftMost >= choicesInRightMost {
			useDirect = true
		} else {
			useDirect = false
		}
	} else {
		if directNodeCount > reverseNodeCount {
			useDirect = true
		} else if reverseNodeCount > directNodeCount {
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

func (splitFinder *SplitIndexFinder) List() [][]byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.List()
	}

	return EmptyList
}

func (splitFinder *SplitIndexFinder) Series() [][]byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Series()
	}

	return nil
}

func (splitFinder *SplitIndexFinder) Abs(v []byte) []byte {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Abs(v)
	}

	return nil
}

func (splitFinder *SplitIndexFinder) Bytes() ([]byte, error) {
	if splitFinder.useWrapped {
		return splitFinder.wrapped.Bytes()
	}

	return nil, ErrNotImplemented
}
