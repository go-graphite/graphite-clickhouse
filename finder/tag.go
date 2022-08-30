package finder

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type TagState int

const (
	TagRoot     TagState = iota // query = "*"
	TagSkip                     // not _tag prefix
	TagInfoRoot                 // query = "_tag"
	TagList
	TagListSeriesRoot
	TagListSeries
	TagListParam
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
		return where.Eq(field, *q.Param+*q.Value)
	}
	if q.Param != nil {
		return where.HasPrefix(field, *q.Param)
	}
	if q.Value != nil && *q.Value != "*" {
		return where.Eq(field, *q.Value)
	}

	return ""
}

type TagFinder struct {
	wrapped     Finder
	url         string             // clickhouse dsn
	table       string             // graphite_tag table
	opts        clickhouse.Options // clickhouse timeout, connectTimeout, etc
	state       TagState
	tagQuery    []TagQ
	seriesQuery string
	tagPrefix   []byte
	body        []byte // clickhouse response
}

var EmptyList [][]byte = [][]byte{}

func WrapTag(f Finder, url string, table string, opts clickhouse.Options) *TagFinder {
	return &TagFinder{
		wrapped:  f,
		url:      url,
		table:    table,
		opts:     opts,
		tagQuery: make([]TagQ, 0),
	}
}

func (t *TagFinder) tagListSQL() (string, error) {
	if len(t.tagQuery) == 0 {
		return "", nil
	}

	w := where.New()

	// first
	w.And(t.tagQuery[0].Where("Tag1"))

	if len(t.tagQuery) == 1 {
		w.And(where.Eq("Level", 1))
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

	w.And(where.Eq("IsLeaf", 1))

	return fmt.Sprintf("SELECT TagN FROM %s ARRAY JOIN Tags AS TagN WHERE %s GROUP BY TagN", t.table, w), nil
}

func (t *TagFinder) seriesSQL() (string, error) {
	if len(t.tagQuery) == 0 {
		return "", nil
	}

	w := where.New()

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
	w.And(base.where(t.seriesQuery).String())

	// TODO: consider consistent query generator
	return fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path FORMAT TabSeparatedRaw", t.table, w), nil
}

func (t *TagFinder) MakeSQL(query string) (string, error) {
	if query == "_tag" {
		t.state = TagInfoRoot
		return "", nil
	}

	qs0 := strings.Split(query, ".")
	qs := qs0

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

	if len(qs0) > len(qs) {
		t.tagPrefix = append([]byte(strings.Join(qs0[:len(qs0)-len(qs)], ".")), '.')
	}

	if t.seriesQuery == "" {
		if len(t.tagQuery) > 0 && t.tagQuery[len(t.tagQuery)-1].Param != nil {
			t.state = TagListParam
		} else {
			t.state = TagList
		}
		return t.tagListSQL()
	}

	if t.seriesQuery == "*" {
		t.state = TagListSeriesRoot
		return t.seriesSQL()
	}

	t.state = TagListSeries
	return t.seriesSQL()
}

func (t *TagFinder) Execute(ctx context.Context, query string, from int64, until int64) error {
	t.state = TagSkip

	if query == "" {
		return t.wrapped.Execute(ctx, query, from, until)
	}

	if query == "*" {
		t.state = TagRoot
		return t.wrapped.Execute(ctx, query, from, until)
	}

	if !strings.HasPrefix(query, "_tag.") && query != "_tag" {
		return t.wrapped.Execute(ctx, query, from, until)
	}

	sql, err := t.MakeSQL(query)
	if err != nil {
		return err
	}

	if sql != "" {
		t.body, err = clickhouse.Query(scope.WithTable(ctx, t.table), t.url, sql, t.opts, nil)
	}

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

	if t.state == TagList || t.state == TagListParam {
		// add dots
		for i := 0; i < len(rows); i++ {
			eqIndex := bytes.IndexByte(rows[i], '=')
			if eqIndex > 0 && eqIndex < len(rows[i])-1 {
				if t.state == TagListParam {
					rows[i] = append(rows[i][eqIndex+1:], '.')
				} else {
					rows[i][eqIndex+1] = '.'
					rows[i] = rows[i][:eqIndex+2]
				}
			} else {
				rows[i] = append(rows[i], '.')
			}
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
		return t.wrapped.Series()
	case TagInfoRoot:
		return EmptyList
	case TagRoot:
		return t.wrapped.Series()
	}

	rows := t.List()

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}
		if rows[i][len(rows[i])-1] == '.' {
			skip++
			continue
		}
		if skip > 0 {
			rows[i-skip] = rows[i]
		}
	}

	return rows
}

func (t *TagFinder) Abs(v []byte) []byte {
	if t.state == TagSkip {
		return t.wrapped.Abs(v)
	}

	return bytesConcat(t.tagPrefix, v)
}

func (t *TagFinder) Bytes() ([]byte, error) {
	return nil, ErrNotImplemented
}
