package finder

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/errs"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"

	"github.com/msaf1980/go-stringutils"
)

var (
	// ErrEmptyArgs         = errors.New("empty arguments")
	ErrCostlySeriesByTag = errs.NewErrorWithCode("seriesByTag argument is too costly", http.StatusForbidden)
)

type TaggedTermOp int

const (
	TaggedTermEq       TaggedTermOp = 1
	TaggedTermMatch    TaggedTermOp = 2
	TaggedTermNe       TaggedTermOp = 3
	TaggedTermNotMatch TaggedTermOp = 4
)

type TaggedTerm struct {
	Key         string
	Op          TaggedTermOp
	Value       string
	HasWildcard bool // only for TaggedTermEq

	NonDefaultCost bool
	Cost           int // tag cost for use ad primary filter (use tag with maximal selectivity). 0 by default, minimal is better.
	// __name__ tag is prefered, if some tag has better selectivity than name, set it cost to < 0
	// values with wildcards or regex matching also has lower priority, set if needed it cost to < 0
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

	if s[i].Op == TaggedTermEq && !s[i].HasWildcard && s[j].HasWildcard {
		// globs as fist eq might be have a bad perfomance
		return true
	}

	if s[i].Key == "__name__" && s[j].Key != "__name__" {
		return true
	}
	return false
}

type TaggedFinder struct {
	url            string                   // clickhouse dsn
	table          string                   // graphite_tag table
	absKeepEncoded bool                     // Abs returns url encoded value. For queries from prometheus
	opts           clickhouse.Options       // clickhouse query timeout
	taggedCosts    map[string]*config.Costs // costs for taggs (sor tune index search)
	dailyEnabled   bool

	body []byte // clickhouse response
}

func NewTagged(url string, table string, dailyEnabled bool, absKeepEncoded bool, opts clickhouse.Options, taggedCosts map[string]*config.Costs) *TaggedFinder {
	return &TaggedFinder{
		url:            url,
		table:          table,
		absKeepEncoded: absKeepEncoded,
		opts:           opts,
		taggedCosts:    taggedCosts,
		dailyEnabled:   dailyEnabled,
	}
}

func (term *TaggedTerm) concat() string {
	return term.Key + "=" + term.Value
}

func (term *TaggedTerm) concatMask() string {
	v := strings.ReplaceAll(term.Value, "*", "%")
	return fmt.Sprintf("%s=%s", term.Key, v)
}

func TaggedTermWhere1(term *TaggedTerm) (string, error) {
	// positive expression check only in Tag1
	// negative check in all Tags
	switch term.Op {
	case TaggedTermEq:
		if strings.Index(term.Value, "*") >= 0 {
			return where.Like("Tag1", term.concatMask()), nil
		}
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
		if strings.Index(term.Value, "*") >= 0 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Like("x", term.concatMask())), nil
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
		return where.Match("Tag1", term.Key, term.Value), nil
	case TaggedTermNotMatch:
		// return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", term.Key, term.Value), nil
		return "NOT " + where.Match("Tag1", term.Key, term.Value), nil
	default:
		return "", nil
	}
}

func TaggedTermWhereN(term *TaggedTerm) (string, error) {
	// arrayExists((x) -> %s, Tags)
	switch term.Op {
	case TaggedTermEq:
		if strings.Index(term.Value, "*") >= 0 {
			return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.Like("x", term.concatMask())), nil
		}
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
		if strings.Index(term.Value, "*") >= 0 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Like("x", term.concatMask())), nil
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
	case TaggedTermMatch:
		return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.Match("x", term.Key, term.Value)), nil
	case TaggedTermNotMatch:
		return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Match("x", term.Key, term.Value)), nil
	default:
		return "", nil
	}
}

func setCost(term *TaggedTerm, costs *config.Costs) {
	if term.Op == TaggedTermEq || term.Op == TaggedTermMatch {
		if len(costs.ValuesCost) > 0 {
			if cost, ok := costs.ValuesCost[term.Value]; ok {
				term.Cost = cost
				term.NonDefaultCost = true
				return
			}
		}
		if term.Op == TaggedTermEq && !term.HasWildcard && costs.Cost != nil {
			term.Cost = *costs.Cost // only for non-wildcared eq
			term.NonDefaultCost = true
		}
	}
}

func ParseTaggedConditions(conditions []string, config *config.Config) ([]TaggedTerm, error) {
	nonWildcards := 0
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
			terms[i].HasWildcard = where.HasWildcard(terms[i].Value)
			if !terms[i].HasWildcard {
				nonWildcards++
			}
		case "!=":
			terms[i].Op = TaggedTermNe
		case "=~":
			terms[i].Op = TaggedTermMatch
		case "!=~":
			terms[i].Op = TaggedTermNotMatch
		default:
			return nil, fmt.Errorf("wrong seriesByTag expr: %#v", s)
		}
		if len(config.ClickHouse.TaggedCosts) > 0 {
			if costs, ok := config.ClickHouse.TaggedCosts[terms[i].Key]; ok {
				setCost(&terms[i], costs)
			}
		}
	}
	if config.ClickHouse.TagsMinInQuery > 0 && nonWildcards < config.ClickHouse.TagsMinInQuery {
		return nil, ErrCostlySeriesByTag
	}

	if len(config.ClickHouse.TaggedCosts) == 0 {
		sort.Sort(TaggedTermList(terms))
	} else {
		// compare with taggs costs
		sort.Slice(terms, func(i, j int) bool {
			// compare taggs costs, if all of TaggegTerms has custom cost.
			// this is allow overwrite operators order (Eq with or without wildcards/Match), use with carefully
			if terms[i].Cost != terms[j].Cost {
				if terms[i].NonDefaultCost && terms[j].NonDefaultCost ||
					(terms[i].NonDefaultCost && terms[j].Op == TaggedTermEq && !terms[j].HasWildcard) ||
					(terms[j].NonDefaultCost && terms[i].Op == TaggedTermEq && !terms[i].HasWildcard) {
					return terms[i].Cost < terms[j].Cost
				}
			}

			if terms[i].Op == terms[j].Op {
				if terms[i].Op == TaggedTermEq && !terms[i].HasWildcard && terms[j].HasWildcard {
					// globs as fist eq might be have a bad perfomance
					return true
				}

				if terms[i].Key == "__name__" && terms[j].Key != "__name__" {
					return true
				}

				if terms[i].Cost != terms[j].Cost && terms[i].HasWildcard == terms[j].HasWildcard {
					// compare taggs costs
					return terms[i].Cost < terms[j].Cost
				}

				return false
			} else {
				return terms[i].Op < terms[j].Op
			}
		})
	}

	return terms, nil
}

var ErrInvalidSeriesByTag = errs.NewErrorWithCode("wrong seriesByTag call", http.StatusBadRequest)

func parseString(s string) (string, string, error) {
	if s[0] != '\'' && s[0] != '"' {
		panic("string should start with open quote")
	}

	match := s[0]

	s = s[1:]

	var i int
	for i < len(s) && s[i] != match {
		i++
	}

	if i == len(s) {
		return "", "", errs.NewErrorfWithCode(http.StatusBadRequest, "seriesByTag arg missing quote %q'", s)
	}

	return s[:i], s[i+1:], nil
}

func seriesByTagArgs(query string) ([]string, error) {
	var err error
	args := make([]string, 0, 8)

	// trim spaces
	e := strings.Trim(query, " ")
	if !strings.HasPrefix(e, "seriesByTag(") {
		return nil, ErrInvalidSeriesByTag
	}
	if e[len(e)-1] != ')' {
		return nil, ErrInvalidSeriesByTag
	}
	e = e[12 : len(e)-1]

	for len(e) > 0 {
		var arg string
		if e[0] == '\'' || e[0] == '"' {
			if arg, e, err = parseString(e); err != nil {
				return nil, err
			}
			// skip empty arg
			if arg != "" {
				args = append(args, arg)
			}
		} else if e[0] == ' ' || e[0] == ',' {
			e = e[1:]
		} else {
			return nil, errs.NewErrorfWithCode(http.StatusBadRequest, "seriesByTag arg missing quote %q", e)
		}
	}
	return args, nil
}

func ParseSeriesByTag(query string, config *config.Config) ([]TaggedTerm, error) {
	conditions, err := seriesByTagArgs(query)
	if err != nil {
		return nil, err
	}

	if len(conditions) < 1 {
		return nil, ErrInvalidSeriesByTag
	}

	return ParseTaggedConditions(conditions, config)
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

func NewCachedTags(body []byte) *TaggedFinder {
	return &TaggedFinder{
		body: body,
	}
}

func (t *TaggedFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) error {
	terms, err := ParseSeriesByTag(query, config)
	if err != nil {
		return err
	}

	return t.ExecutePrepared(ctx, terms, from, until, stat)
}

func (t *TaggedFinder) whereFilter(terms []TaggedTerm, from int64, until int64) (*where.Where, *where.Where, error) {
	w, pw, err := TaggedWhere(terms)
	if err != nil {
		return nil, nil, err
	}

	if t.dailyEnabled {
		w.Andf(
			"Date >='%s' AND Date <= '%s'",
			date.FromTimestampToDaysFormat(from),
			date.UntilTimestampToDaysFormat(until),
		)
	}
	return w, pw, nil
}

func (t *TaggedFinder) ExecutePrepared(ctx context.Context, terms []TaggedTerm, from int64, until int64, stat *FinderStat) error {
	w, pw, err := t.whereFilter(terms, from, until)
	if err != nil {
		return err
	}
	// TODO: consider consistent query generator
	sql := fmt.Sprintf("SELECT Path FROM %s %s %s GROUP BY Path FORMAT TabSeparatedRaw", t.table, pw.PreWhereSQL(), w.SQL())
	t.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(scope.WithTable(ctx, t.table), t.url, sql, t.opts, nil)
	stat.Table = t.table
	stat.ReadBytes = int64(len(t.body))
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

func tagsParse(path string) (string, []string, error) {
	name, args, n := stringutils.Split2(path, "?")
	if n == 1 || args == "" {
		return name, nil, fmt.Errorf("incomplete tags in '%s'", path)
	}
	tags := strings.Split(args, "&")
	for i := range tags {
		tags[i] = unescape(tags[i])
	}
	return unescape(name), tags, nil
}

func TaggedDecode(v []byte) []byte {
	s := stringutils.UnsafeString(v)
	name, tags, err := tagsParse(s)
	if err != nil {
		return v
	}

	if len(tags) == 0 {
		return stringutils.UnsafeStringBytes(&name)
	}
	sort.Strings(tags)

	var sb stringutils.Builder

	length := len(name)
	for _, tag := range tags {
		length += len(tag) + 1
	}

	sb.Grow(length)

	sb.WriteString(name)
	for _, tag := range tags {
		sb.WriteString(";")
		sb.WriteString(tag)
	}
	return sb.Bytes()
}

func (t *TaggedFinder) Abs(v []byte) []byte {
	if t.absKeepEncoded {
		return v
	}

	return TaggedDecode(v)
}

func (t *TaggedFinder) Bytes() ([]byte, error) {
	return nil, ErrNotImplemented
}
