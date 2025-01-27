package finder

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/errs"
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

func useDaily(dailyEnabled bool, from, until int64) bool {
	return dailyEnabled && from > 0 && until > 0
}

func calculateIndexLevelOffset(useDaily, reverse bool) int {
	var levelOffset int
	if useDaily {
		if reverse {
			levelOffset = ReverseLevelOffset
		}
	} else if reverse {
		levelOffset = ReverseTreeLevelOffset
	} else {
		levelOffset = TreeLevelOffset
	}

	return levelOffset
}

func addDatesToWhere(w *where.Where, useDaily bool, from, until int64) {
	if useDaily {
		w.Andf(
			"Date >='%s' AND Date <= '%s'",
			date.FromTimestampToDaysFormat(from),
			date.UntilTimestampToDaysFormat(until),
		)
	} else {
		w.And(where.Eq("Date", DefaultTreeDate))
	}
}

func (idx *IndexFinder) whereFilter(query string, from int64, until int64) *where.Where {
	reverse := idx.useReverse(query)
	if reverse {
		query = ReverseString(query)
	}

	idx.useDaily = useDaily(idx.dailyEnabled, from, until)

	levelOffset := calculateIndexLevelOffset(idx.useDaily, reverse)

	w := idx.where(query, levelOffset)
	addDatesToWhere(w, idx.useDaily, from, until)
	return w
}

func validatePlainQuery(query string, wildcardMinDistance int) error {
	if where.HasUnmatchedBrackets(query) {
		return errs.NewErrorWithCode("query has unmatched brackets", http.StatusBadRequest)
	}

	var maxDist = where.MaxWildcardDistance(query)

	// If the amount of nodes in a plain query is equal to 1,
	// then make an exception
	// This allows to check which root nodes exist
	moreThanOneNode := strings.Count(query, ".") >= 1

	if maxDist != -1 && maxDist < wildcardMinDistance && moreThanOneNode {
		return errs.NewErrorWithCode("query has wildcards way too early at the start and at the end of it", http.StatusBadRequest)
	}

	return nil
}

func (idx *IndexFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	err = validatePlainQuery(query, config.ClickHouse.WildcardMinDistance)
	if err != nil {
		return err
	}
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

func splitIndexBody(body []byte, useReverse, useCache bool) ([]byte, [][]byte, bool) {
	if len(body) == 0 {
		return body, [][]byte{}, false
	}

	rows := bytes.Split(bytes.TrimSuffix(body, []byte{'\n'}), []byte{'\n'})
	setDirect := false

	if useReverse {
		var buf bytes.Buffer
		if useCache {
			buf.Grow(len(body))
		}

		for i := range rows {
			rows[i] = ReverseBytes(rows[i])
			if useCache {
				buf.Write(rows[i])
				buf.WriteByte('\n')
			}
		}

		if useCache {
			body = buf.Bytes()
			setDirect = true
		}
	}

	return body, rows, setDirect
}

func (idx *IndexFinder) bodySplit() {
	setDirect := false
	idx.body, idx.rows, setDirect = splitIndexBody(idx.body, idx.useReverse(""), idx.useCache)
	if setDirect {
		idx.reverse = queryDirect
	}

	//if len(idx.body) == 0 {
	//	return
	//}
	//
	//idx.rows = bytes.Split(bytes.TrimSuffix(idx.body, []byte{'\n'}), []byte{'\n'})
	//
	//if idx.useReverse("") {
	//	// rotate names for reduce
	//	var buf bytes.Buffer
	//	if idx.useCache {
	//		buf.Grow(len(idx.body))
	//	}
	//	for i := 0; i < len(idx.rows); i++ {
	//		idx.rows[i] = ReverseBytes(idx.rows[i])
	//		if idx.useCache {
	//			buf.Write(idx.rows[i])
	//			buf.WriteByte('\n')
	//		}
	//	}
	//	if idx.useCache {
	//		idx.body = buf.Bytes()
	//		idx.reverse = queryDirect
	//	}
	//}
}

func makeList(rows [][]byte, onlySeries bool) [][]byte {
	if len(rows) == 0 {
		return [][]byte{}
	}

	resRows := make([][]byte, len(rows))

	for i := 0; i < len(rows); i++ {
		resRows[i] = rows[i]
	}

	return resRows
}

func (idx *IndexFinder) makeList(onlySeries bool) [][]byte {
	return makeList(idx.rows, onlySeries)
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
