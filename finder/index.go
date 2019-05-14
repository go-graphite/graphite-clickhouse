package finder

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
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
	body         []byte // clickhouse response body
	useReverse   bool
	useDaily     bool
}

func NewIndex(url string, table string, dailyEnabled bool, opts clickhouse.Options) Finder {
	return &IndexFinder{
		url:          url,
		table:        table,
		opts:         opts,
		dailyEnabled: dailyEnabled,
	}
}

func (idx *IndexFinder) where(query string, levelOffset int) *Where {
	level := strings.Count(query, ".") + 1

	w := NewWhere()

	w.Andf("Level = %d", level+levelOffset)

	if query == "*" {
		return w
	}

	// simple metric
	if !HasWildcard(query) {
		w.Andf("Path = %s OR Path = %s", Q(query), Q(query+"."))
		return w
	}

	// before any wildcard symbol
	simplePrefix := query[:strings.IndexAny(query, "[]{}*?")]

	if len(simplePrefix) > 0 {
		w.Andf("Path LIKE %s", Q(likeEscape(simplePrefix)+`%`))
	}

	// prefix search like "metric.name.xx*"
	if len(simplePrefix) == len(query)-1 && query[len(query)-1] == '*' {
		return w
	}

	// Q() replaces \ with \\, so using \. does not work here.
	// work around with [.]
	w.Andf("match(Path, %s)", Q(`^`+GlobToRegexp(query)+`[.]?$`))
	return w
}

func (idx *IndexFinder) Execute(ctx context.Context, query string, from int64, until int64) (err error) {
	p := strings.LastIndexByte(query, '.')

	if p < 0 || p >= len(query)-1 || HasWildcard(query[p+1:]) {
		idx.useReverse = false
	} else {
		idx.useReverse = true
	}

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

	where := idx.where(query, levelOffset)

	if idx.useDaily {
		where.Andf(
			"Date >='%s' AND Date <= '%s'",
			time.Unix(from, 0).Format("2006-01-02"),
			time.Unix(until, 0).Format("2006-01-02"),
		)
	} else {
		where.Andf("Date = '%s'", DefaultTreeDate)
	}

	idx.body, err = clickhouse.Query(
		ctx,
		idx.url,
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", idx.table, where),
		idx.table,
		idx.opts,
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
