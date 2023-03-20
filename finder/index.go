package finder

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

const ReverseLevelOffset = 10000
const TreeLevelOffset = 20000
const ReverseTreeLevelOffset = 30000

const DefaultTreeDate = "1970-02-12"

const (
	queryAuto     = config.IndexAuto
	queryDirect   = config.IndexDirect
	queryReversed = config.IndexReversed
)

type IndexFinder struct {
	url          string             // clickhouse dsn
	table        string             // graphite_tree table
	opts         clickhouse.Options // timeout, connectTimeout
	dailyEnabled bool
	confReverse  uint8
	confReverses config.IndexReverses
	reverse      uint8  // calculated in IndexFinder.useReverse only once
	body         []byte // clickhouse response body
	rows         [][]byte
	useCache     bool // rotate body if needed (for store in cache)
	useDaily     bool
}

func NewCachedIndex(body []byte) Finder {
	idx := &IndexFinder{
		body:    body,
		reverse: queryDirect,
	}
	idx.bodySplit()

	return idx
}

func NewIndex(url string, table string, dailyEnabled bool, reverse string, reverses config.IndexReverses, opts clickhouse.Options, useCache bool) Finder {
	return &IndexFinder{
		url:          url,
		table:        table,
		opts:         opts,
		dailyEnabled: dailyEnabled,
		confReverse:  config.IndexReverse[reverse],
		confReverses: reverses,
		useCache:     useCache,
	}
}

func (idx *IndexFinder) where(query string, levelOffset int) *where.Where {
	level := strings.Count(query, ".") + 1

	w := where.New()

	w.And(where.Eq("Level", level+levelOffset))
	w.And(where.TreeGlob("Path", query))

	return w
}

func (idx *IndexFinder) checkReverses(query string) uint8 {
	for _, rule := range idx.confReverses {
		if len(rule.Prefix) > 0 && !strings.HasPrefix(query, rule.Prefix) {
			continue
		}
		if len(rule.Suffix) > 0 && !strings.HasSuffix(query, rule.Suffix) {
			continue
		}
		if rule.Regex != nil && rule.Regex.FindStringIndex(query) == nil {
			continue
		}
		return config.IndexReverse[rule.Reverse]
	}
	return idx.confReverse
}

func (idx *IndexFinder) useReverse(query string) bool {
	if idx.reverse == queryDirect {
		return false
	} else if idx.reverse == queryReversed {
		return true
	}

	if idx.reverse = idx.checkReverses(query); idx.reverse != queryAuto {
		return idx.useReverse(query)
	}

	w := where.IndexWildcard(query)
	if w == -1 {
		idx.reverse = queryDirect
		return idx.useReverse(query)
	}
	firstWildcardNode := strings.Count(query[:w], ".")

	w = where.IndexLastWildcard(query)
	lastWildcardNode := strings.Count(query[w:], ".")

	if firstWildcardNode < lastWildcardNode {
		idx.reverse = queryReversed
		return idx.useReverse(query)
	}
	idx.reverse = queryDirect
	return idx.useReverse(query)
}

func (idx *IndexFinder) whereFilter(query string, from int64, until int64) *where.Where {
	reverse := idx.useReverse(query)
	if reverse {
		query = ReverseString(query)
	}

	if idx.dailyEnabled && from > 0 && until > 0 {
		idx.useDaily = true
	} else {
		idx.useDaily = false
	}

	var levelOffset int
	if idx.useDaily {
		if reverse {
			levelOffset = ReverseLevelOffset
		}
	} else if reverse {
		levelOffset = ReverseTreeLevelOffset
	} else {
		levelOffset = TreeLevelOffset
	}

	w := idx.where(query, levelOffset)
	if idx.useDaily {
		w.Andf(
			"Date >='%s' AND Date <= '%s'",
			date.FromTimestampToDaysFormat(from),
			date.UntilTimestampToDaysFormat(until),
		)
	} else {
		w.And(where.Eq("Date", DefaultTreeDate))
	}
	return w
}

func (idx *IndexFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	w := idx.whereFilter(query, from, until)

	idx.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(
		scope.WithTable(ctx, idx.table),
		idx.url,
		// TODO: consider consistent query generator
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path FORMAT TabSeparatedRaw", idx.table, w),
		idx.opts,
		nil,
	)
	stat.Table = idx.table
	if err == nil {
		stat.ReadBytes = int64(len(idx.body))
		idx.bodySplit()
	}

	return
}

func (idx *IndexFinder) Abs(v []byte) []byte {
	return v
}

func (idx *IndexFinder) bodySplit() {
	idx.rows = bytes.Split(idx.body, []byte{'\n'})

	if idx.useReverse("") {
		// rotate names for reduce
		var buf bytes.Buffer
		if idx.useCache {
			buf.Grow(len(idx.body))
		}
		for i := 0; i < len(idx.rows); i++ {
			idx.rows[i] = ReverseBytes(idx.rows[i])
			if idx.useCache {
				buf.Write(idx.rows[i])
				buf.WriteByte('\n')
			}
		}
		if idx.useCache {
			idx.body = buf.Bytes()
			idx.reverse = queryDirect
		}
	}
}

func (idx *IndexFinder) makeList(onlySeries bool) [][]byte {
	if len(idx.rows) == 0 {
		return [][]byte{}
	}

	rows := make([][]byte, len(idx.rows))

	for i := 0; i < len(idx.rows); i++ {
		rows[i] = idx.rows[i]
	}

	return rows
}

func (idx *IndexFinder) List() [][]byte {
	return idx.makeList(false)
}

func (idx *IndexFinder) Series() [][]byte {
	return idx.makeList(true)
}

func (idx *IndexFinder) Bytes() ([]byte, error) {
	return idx.body, nil
}
