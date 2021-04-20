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
	revUse       []config.NValue
	body         []byte // clickhouse response body
	useReverse   bool
	useDaily     bool
}

func NewIndex(url string, table string, dailyEnabled bool, reverseDepth int, reverseUse []config.NValue, opts clickhouse.Options) Finder {
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

func useReverseDepth(query string, reverseDepth int, revUse []config.NValue) bool {
	p := strings.LastIndexByte(query, '.')

	if !where.HasWildcard(query) || p < 0 || p >= len(query)-1 || where.HasWildcard(query[p+1:]) {
		return false
	}
	return true
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
