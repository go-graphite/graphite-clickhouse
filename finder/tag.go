package finder

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type TagState int

const (
	TagRoot     TagState = iota // query = "*"
	TagSkip                     // not _tag prefix
	TagInfoRoot                 // query = "_tag"
	TagList
	TagListSeriesRoot
	TagListSeries
)

type TagQ struct {
	Param *string
	Value *string
}

func (q TagQ) String() string {
	if q.Param != nil && q.Value != nil {
		return fmt.Sprintf("{\"param\"=%#v, \"value\"=%#v}", *q.Param, *q.Value)
	}
	if q.Param != nil {
		return fmt.Sprintf("{\"param\"=%#v}", *q.Param)
	}
	if q.Value != nil {
		return fmt.Sprintf("{\"value\"=%#v}", *q.Value)
	}
	return "{}"
}

func (q *TagQ) Where(field string) string {
	if q.Param != nil && q.Value != nil && *q.Value != "*" {
		return fmt.Sprintf("%s=%s", field, Q(*q.Param+*q.Value))
	}
	if q.Param != nil {
		return fmt.Sprintf("%s LIKE %s", field, Q(*q.Param+`%`))
	}
	if q.Value != nil && *q.Value != "*" {
		return fmt.Sprintf("%s=%s", field, Q(*q.Value))
	}

	return ""
}

type TagFinder struct {
	wrapped     Finder
	ctx         context.Context // for clickhouse.Query
	url         string          // clickhouse dsn
	table       string          // graphite_tag table
	timeout     time.Duration   // clickhouse query timeout
	state       TagState
	tagQuery    []TagQ
	seriesQuery string
	body        []byte // clickhouse response
}

var EmptyList [][]byte = [][]byte{}

func WrapTag(f Finder, ctx context.Context, url string, table string, timeout time.Duration) *TagFinder {
	return &TagFinder{
		wrapped:  f,
		ctx:      ctx,
		url:      url,
		table:    table,
		timeout:  timeout,
		tagQuery: make([]TagQ, 0),
	}
}

func (t *TagFinder) tagListSQL() (string, error) {
	if len(t.tagQuery) == 0 {
		return "", nil
	}

	w := NewWhere()

	w.Andf("Version>=(SELECT Max(Version) FROM %s WHERE Tag1='' AND Level=0 AND Path='')", t.table)

	// first
	w.And(t.tagQuery[0].Where("Tag1"))

	if len(t.tagQuery) == 1 {
		w.And("Level=1")
		return fmt.Sprintf("SELECT Tag1 FROM %s WHERE %s GROUP BY Tag1", t.table, w), nil
	}

	// 1..(n-1)
	for i := 1; i < len(t.tagQuery)-1; i++ {
		cond := t.tagQuery[i].Where("x")
		if cond != "" {
			w.Andf("arrayExists((x) -> %s, Tags)", cond)
		}
	}

	// last
	w.And(t.tagQuery[len(t.tagQuery)-1].Where("TagN"))

	w.And("IsLeaf=1")

	return fmt.Sprintf("SELECT TagN FROM %s ARRAY JOIN Tags AS TagN WHERE %s GROUP BY TagN", t.table, w), nil
}

func (t *TagFinder) seriesSQL() (string, error) {
	if len(t.tagQuery) == 0 {
		return "", nil
	}

	w := NewWhere()

	w.Andf("Version>=(SELECT Max(Version) FROM %s WHERE Tag1='' AND Level=0 AND Path='')", t.table)
	// first
	w.And(t.tagQuery[0].Where("Tag1"))

	// 1..(n-1)
	for i := 1; i < len(t.tagQuery); i++ {
		cond := t.tagQuery[i].Where("x")
		if cond != "" {
			w.Andf("arrayExists((x) -> %s, Tags)", cond)
		}
	}

	base := &BaseFinder{}
	w.And(base.where(t.seriesQuery))

	return fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", t.table, w), nil
}

func (t *TagFinder) MakeSQL(query string) (string, error) {
	if query == "_tag" {
		return "", nil
	}

	qs := strings.Split(query, ".")

	t.tagQuery = make([]TagQ, 0)

	for {
		if len(qs) == 0 {
			break
		}
		if qs[0] == "_tag" {
			if len(qs) >= 2 {
				v := qs[1]
				if len(v) > 0 && v[len(v)-1] == '=' {
					if len(qs) >= 3 {
						t.tagQuery = append(t.tagQuery, TagQ{Param: &v, Value: &qs[2]})
						qs = qs[3:]
					} else {
						t.tagQuery = append(t.tagQuery, TagQ{Param: &v})
						qs = qs[2:]
					}
				} else {
					t.tagQuery = append(t.tagQuery, TagQ{Value: &v})
					qs = qs[2:]
				}
			} else {
				t.tagQuery = append(t.tagQuery, TagQ{})
				qs = qs[1:]
			}
		} else {
			t.seriesQuery = strings.Join(qs, ".")
			break
		}
	}

	if t.seriesQuery == "" {
		t.state = TagList
		return t.tagListSQL()
	}

	if t.seriesQuery == "*" {
		t.state = TagListSeriesRoot
		return t.seriesSQL()
	}

	t.state = TagListSeries
	return t.seriesSQL()
}

func (t *TagFinder) Execute(query string) error {
	t.state = TagSkip

	if query == "" {
		return t.wrapped.Execute(query)
	}

	if query == "*" {
		t.state = TagRoot
		return t.wrapped.Execute(query)
	}

	if !strings.HasPrefix(query, "_tag.") {
		return t.wrapped.Execute(query)
	}

	sql, err := t.MakeSQL(query)
	if err != nil {
		return err
	}

	t.body, err = clickhouse.Query(t.ctx, t.url, sql, t.timeout)

	return err
}

func (t *TagFinder) List() [][]byte {
	switch t.state {
	case TagSkip:
		return t.wrapped.List()
	case TagInfoRoot:
		return [][]byte{[]byte("_tag.")}
	case TagRoot:
		// pass
		return append([][]byte{[]byte("_tag.")}, t.wrapped.List()...)
	}

	if t.body == nil {
		return [][]byte{}
	}

	rows := bytes.Split(t.body, []byte{'\n'})

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}
		if skip > 0 {
			rows[i-skip] = rows[i]
		}
	}

	rows = rows[:len(rows)-skip]

	if t.state == TagList {
		// add dots
		// @TODO: optimize?
		for i := 0; i < len(rows); i++ {
			rows[i] = append(rows[i], '.')
		}
	}

	if t.state == TagListSeriesRoot {
		rows = append(rows, []byte("_tag."))
	}

	return rows
}

func (t *TagFinder) Series() [][]byte {
	switch t.state {
	case TagSkip:
		return t.wrapped.List()
	case TagInfoRoot:
		return EmptyList
	case TagRoot:
		return t.wrapped.Series()
	default:
		return EmptyList
	}

	return EmptyList
}

func (t *TagFinder) Abs(v []byte) []byte {
	// @TODO
	return v
}
