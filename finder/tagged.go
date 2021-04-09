package finder

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type TaggedTermOp int

const (
	TaggedTermEq       TaggedTermOp = 1
	TaggedTermMatch    TaggedTermOp = 2
	TaggedTermNe       TaggedTermOp = 3
	TaggedTermNotMatch TaggedTermOp = 4
)

type TaggedTerm struct {
	Key   string
	Op    TaggedTermOp
	Value string
}

type TaggedTermList []TaggedTerm

func (s TaggedTermList) Len() int {
	return len(s)
}
func (s TaggedTermList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s TaggedTermList) Less(i, j int) bool {
	if s[i].Op < s[j].Op {
		return true
	}
	if s[i].Op > s[j].Op {
		return false
	}
	if s[i].Key == "__name__" && s[j].Key != "__name__" {
		return true
	}
	return false
}

type TaggedFinder struct {
	url            string             // clickhouse dsn
	table          string             // graphite_tag table
	absKeepEncoded bool               // Abs returns url encoded value. For queries from prometheus
	opts           clickhouse.Options // clickhouse query timeout
	body           []byte             // clickhouse response
}

func NewTagged(url string, table string, absKeepEncoded bool, opts clickhouse.Options) *TaggedFinder {
	return &TaggedFinder{
		url:            url,
		table:          table,
		absKeepEncoded: absKeepEncoded,
		opts:           opts,
	}
}

func (term *TaggedTerm) concat() string {
	return fmt.Sprintf("%s=%s", term.Key, term.Value)
}

func TaggedTermWhere1(term *TaggedTerm) (string, error) {
	// positive expression check only in Tag1
	// negative check in all Tags
	switch term.Op {
	case TaggedTermEq:
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return where.Eq("Tag1", values[0]), nil
		} else if len(values) > 1 {
			return where.In("Tag1", values), nil
		} else {
			return where.Eq("Tag1", term.concat()), nil
		}
	case TaggedTermNe:
		if term.Value == "" {
			// special case
			// container_name!=""  ==> container_name exists and it is not empty
			return where.HasPrefixAndNotEq("Tag1", term.Key+"="), nil
		}
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Eq("x", values[0])), nil
		} else if len(values) > 1 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.In("x", values)), nil
		} else {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Eq("x", term.concat())), nil
		}
	case TaggedTermMatch:
		return where.Match("Tag1", term.concat()), nil
	case TaggedTermNotMatch:
		return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Match("x", term.concat())), nil
	default:
		return "", nil
	}
}

func TaggedTermWhereN(term *TaggedTerm) (string, error) {
	// arrayExists((x) -> %s, Tags)
	switch term.Op {
	case TaggedTermEq:
		//return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.Eq("x", term.concat()))
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return "arrayExists((x) -> " + where.Eq("x", values[0]) + ", Tags)", nil
		} else if len(values) > 1 {
			return "arrayExists((x) -> " + where.In("x", values) + ", Tags)", nil
		} else {
			return "arrayExists((x) -> " + where.Eq("x", term.concat()) + ", Tags)", nil
		}
	case TaggedTermNe:
		if term.Value == "" {
			// special case
			// container_name!=""  ==> container_name exists and it is not empty
			return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.HasPrefixAndNotEq("x", term.Key+"=")), nil
		}
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return "NOT arrayExists((x) -> " + where.Eq("x", values[0]) + ", Tags)", nil
		} else if len(values) > 1 {
			return "NOT arrayExists((x) -> " + where.In("x", values) + ", Tags)", nil
		} else {
			return "NOT arrayExists((x) -> " + where.Eq("x", term.concat()) + ", Tags)", nil
		}

		return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Eq("x", term.concat())), nil
	case TaggedTermMatch:
		return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.Match("x", term.concat())), nil
	case TaggedTermNotMatch:
		return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Match("x", term.concat())), nil
	default:
		return "", nil
	}
}

func ParseTaggedConditions(conditions []string) ([]TaggedTerm, error) {
	terms := make([]TaggedTerm, len(conditions))

	for i := 0; i < len(conditions); i++ {
		s := conditions[i]

		a := strings.SplitN(s, "=", 2)
		if len(a) != 2 {
			return nil, fmt.Errorf("wrong seriesByTag expr: %#v", s)
		}

		a[0] = strings.TrimSpace(a[0])
		a[1] = strings.TrimSpace(a[1])

		op := "="

		if len(a[0]) > 0 && a[0][len(a[0])-1] == '!' {
			op = "!" + op
			a[0] = strings.TrimSpace(a[0][:len(a[0])-1])
		}

		if len(a[1]) > 0 && a[1][0] == '~' {
			op = op + "~"
			a[1] = strings.TrimSpace(a[1][1:])
		}

		terms[i].Key = a[0]
		terms[i].Value = a[1]

		if terms[i].Key == "name" {
			terms[i].Key = "__name__"
		}

		switch op {
		case "=":
			terms[i].Op = TaggedTermEq
		case "!=":
			terms[i].Op = TaggedTermNe
		case "=~":
			terms[i].Op = TaggedTermMatch
		case "!=~":
			terms[i].Op = TaggedTermNotMatch
		default:
			return nil, fmt.Errorf("wrong seriesByTag expr: %#v", s)
		}
	}

	sort.Sort(TaggedTermList(terms))

	return terms, nil
}

func ParseSeriesByTag(query string) ([]TaggedTerm, error) {
	expr, _, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	validationError := fmt.Errorf("wrong seriesByTag call: %#v", query)

	// check
	if !expr.IsFunc() {
		return nil, validationError
	}
	if expr.Target() != "seriesByTag" {
		return nil, validationError
	}

	args := expr.Args()
	if len(args) < 1 {
		return nil, validationError
	}

	for i := 0; i < len(args); i++ {
		if !args[i].IsString() {
			return nil, validationError
		}
	}

	conditions := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		s := args[i].StringValue()
		if s == "" {
			continue
		}
		conditions = append(conditions, s)
	}

	return ParseTaggedConditions(conditions)
}

func TaggedWhere(terms []TaggedTerm) (*where.Where, *where.Where, error) {
	w := where.New()
	pw := where.New()
	x, err := TaggedTermWhere1(&terms[0])
	if err != nil {
		return nil, nil, err
	}
	if terms[0].Op == TaggedTermMatch {
		pw.And(x)
	}
	w.And(x)

	for i := 1; i < len(terms); i++ {
		and, err := TaggedTermWhereN(&terms[i])
		if err != nil {
			return nil, nil, err
		}
		w.And(and)
	}

	return w, pw, nil
}

func (t *TaggedFinder) Execute(ctx context.Context, query string, from int64, until int64) error {
	terms, err := ParseSeriesByTag(query)
	if err != nil {
		return err
	}

	return t.ExecutePrepared(ctx, terms, from, until)
}

func (t *TaggedFinder) ExecutePrepared(ctx context.Context, terms []TaggedTerm, from int64, until int64) error {
	w, pw, err := TaggedWhere(terms)
	if err != nil {
		return err
	}

	w.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(from, 0).Format("2006-01-02"),
		time.Unix(until, 0).Format("2006-01-02"),
	)

	sql := fmt.Sprintf("SELECT Path FROM %s %s %s GROUP BY Path", t.table, pw.PreWhereSQL(), w.SQL())
	t.body, err = clickhouse.Query(scope.WithTable(ctx, t.table), t.url, sql, t.opts, nil)
	return err
}

func (t *TaggedFinder) List() [][]byte {
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

	return rows
}

func (t *TaggedFinder) Series() [][]byte {
	return t.List()
}

func (t *TaggedFinder) Abs(v []byte) []byte {
	if t.absKeepEncoded {
		return v
	}

	u, err := url.Parse(string(v))
	if err != nil {
		return v
	}

	tags := make([]string, 0, len(u.Query()))
	for k, v := range u.Query() {
		tags = append(tags, fmt.Sprintf("%s=%s", k, v[0]))
	}

	sort.Strings(tags)
	if len(tags) == 0 {
		return []byte(u.Path)
	}

	return []byte(fmt.Sprintf("%s;%s", u.Path, strings.Join(tags, ";")))
}
