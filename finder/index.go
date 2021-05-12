package finder

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

const ReverseLevelOffset = 10000
const TreeLevelOffset = 20000
const ReverseTreeLevelOffset = 30000

const DefaultTreeDate = "1970-02-12"

type IndexFinder struct {
	url          string             // clickhouse dsn
	table        string             // graphite_tree table
	opts         clickhouse.Options // timeout, connectTimeout
	dailyEnabled bool
	reverseDepth int
	revUse       []*config.NValue
	body         []byte // clickhouse response body
	useReverse   bool
	useDaily     bool
}

func NewIndex(url string, table string, dailyEnabled bool, reverseDepth int, reverseUse []*config.NValue, opts clickhouse.Options) Finder {
	return &IndexFinder{
		url:          url,
		table:        table,
		opts:         opts,
		dailyEnabled: dailyEnabled,
		reverseDepth: reverseDepth,
		revUse:       reverseUse,
	}
}

func (idx *IndexFinder) where(query string, levelOffset int) *where.Where {
	level := strings.Count(query, ".") + 1

	w := where.New()

	w.And(where.Eq("Level", level+levelOffset))
	w.And(where.TreeGlob("Path", query))

	return w
}

func reverseSuffixDepth(query string, defaultReverseDepth int, revUse []*config.NValue) int {
	for i := range revUse {
		if len(revUse[i].Prefix) > 0 && !strings.HasPrefix(query, revUse[i].Prefix) {
			continue
		}
		if len(revUse[i].Suffix) > 0 && !strings.HasSuffix(query, revUse[i].Suffix) {
			continue
		}
		if revUse[i].Regex != nil && revUse[i].Regex.FindStringIndex(query) == nil {
			continue
		}
		return revUse[i].Value
	}
	return defaultReverseDepth
}

func useReverseDepth(query string, reverseDepth int, revUse []*config.NValue) bool {
	if reverseDepth == -1 {
		return false
	}

	w := where.IndexWildcardOrDot(query)
	if w == -1 {
		return false
	} else if query[w] == '.' {
		reverseDepth = reverseSuffixDepth(query, reverseDepth, revUse)
		if reverseDepth == 0 {
			return false
		} else if reverseDepth == 1 {
			if len(query) <= w+1 {
				return where.HasWildcard(query[:w])
			}
			p := strings.IndexByte(query[w+1:], '.') + w + 1
			return where.HasWildcard(query[:p])
		}
	} else {
		reverseDepth = 1
	}

	w = where.IndexReverseWildcard(query)
	if w == -1 {
		return false
	}
	p := len(query)
	if w == p-1 {
		return false
	}
	depth := 0

	for {
		e := strings.LastIndexByte(query[w:p], '.')
		if e < 0 {
			break
		} else if e < len(query)-1 {
			if where.HasWildcard(query[w+e+1 : p]) {
				break
			}
			depth++
			if depth >= reverseDepth {
				return true
			}
			if e == 0 {
				break
			}
		}
		p = w + e - 1
	}
	return false
}

func (idx *IndexFinder) Execute(ctx context.Context, query string, from int64, until int64) (err error) {
	idx.useReverse = useReverseDepth(query, idx.reverseDepth, idx.revUse)

	if idx.dailyEnabled && from > 0 && until > 0 {
		idx.useDaily = true
	} else {
		idx.useDaily = false
	}

	var levelOffset int
	if idx.useDaily {
		if idx.useReverse {
			levelOffset = ReverseLevelOffset
		}
	} else {
		if idx.useReverse {
			levelOffset = ReverseTreeLevelOffset
		} else {
			levelOffset = TreeLevelOffset
		}
	}

	if idx.useReverse {
		query = ReverseString(query)
	}

	w := idx.where(query, levelOffset)

	if idx.useDaily {
		w.Andf(
			"Date >='%s' AND Date <= '%s'",
			time.Unix(from, 0).Format("2006-01-02"),
			time.Unix(until, 0).Format("2006-01-02"),
		)
	} else {
		w.And(where.Eq("Date", DefaultTreeDate))
	}

	idx.body, err = clickhouse.Query(
		scope.WithTable(ctx, idx.table),
		idx.url,
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", idx.table, w),
		idx.opts,
		nil,
	)

	return
}

func (idx *IndexFinder) Abs(v []byte) []byte {
	return v
}

func (idx *IndexFinder) makeList(onlySeries bool) [][]byte {
	if idx.body == nil {
		return [][]byte{}
	}

	rows := bytes.Split(idx.body, []byte{'\n'})

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}
		if onlySeries && rows[i][len(rows[i])-1] == '.' {
			skip++
			continue
		}
		if skip > 0 {
			rows[i-skip] = rows[i]
		}
	}

	rows = rows[:len(rows)-skip]

	if idx.useReverse {
		for i := 0; i < len(rows); i++ {
			rows[i] = ReverseBytes(rows[i])
		}
	}

	return rows
}

func (idx *IndexFinder) List() [][]byte {
	return idx.makeList(false)
}

func (idx *IndexFinder) Series() [][]byte {
	return idx.makeList(true)
}
