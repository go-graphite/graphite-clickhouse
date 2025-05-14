package where

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unsafe"

	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/errs"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// workaraund for Grafana multi-value variables, expand S{a,b,c}E to [SaE,SbE,ScE]
func GlobExpandSimple(value, prefix string, result *[]string) error {
	if len(value) == 0 {
		// we at the end of glob
		*result = append(*result, prefix)
		return nil
	}

	start := strings.IndexAny(value, "{}")
	if start == -1 {
		*result = append(*result, prefix+value)
	} else {
		end := strings.Index(value[start:], "}")
		if end <= 1 {
			return errs.NewErrorWithCode("malformed glob: "+value, http.StatusBadRequest)
		}
		if end == -1 || strings.IndexAny(value[start+1:start+end], "{}") != -1 {
			return errs.NewErrorWithCode("malformed glob: "+value, http.StatusBadRequest)
		}
		if start > 0 {
			prefix = prefix + value[0:start]
		}
		g := value[start+1 : start+end]
		values := strings.Split(g, ",")
		var postfix string
		if end+start-1 < len(value) {
			postfix = value[start+end+1:]
		}
		for _, v := range values {
			if err := GlobExpandSimple(postfix, prefix+v, result); err != nil {
				return err
			}
		}
	}

	return nil
}

func GlobToRegexp(g string) string {
	s := g
	s = strings.ReplaceAll(s, ".", "[.]")
	s = strings.ReplaceAll(s, "$", "[$]")
	s = strings.ReplaceAll(s, "{", "(")
	s = strings.ReplaceAll(s, "}", ")")
	s = strings.ReplaceAll(s, "?", "[^.]")
	s = strings.ReplaceAll(s, ",", "|")
	s = strings.ReplaceAll(s, "*", "([^.]*?)")
	return s
}

func HasWildcard(target string) bool {
	return strings.IndexAny(target, "[]{}*?") > -1
}

func IndexLastWildcard(target string) int {
	return strings.LastIndexAny(target, "[]{}*?")
}

func IndexWildcard(target string) int {
	return strings.IndexAny(target, "[]{}*?")
}

func MaxWildcardDistance(query string) int {
	if !HasWildcard(query) {
		return -1
	}

	w := IndexWildcard(query)
	firstWildcardNode := strings.Count(query[:w], ".")
	w = IndexLastWildcard(query)
	lastWildcardNode := strings.Count(query[w:], ".")

	return max(firstWildcardNode, lastWildcardNode)
}

func NonRegexpPrefix(expr string) string {
	s := regexp.QuoteMeta(expr)
	for i := 0; i < len(expr); i++ {
		if expr[i] != s[i] || expr[i] == '\\' {
			if len(expr) > i+1 && expr[i] == '|' {
				eq := strings.LastIndexAny(expr[:i], "=~")
				if eq > 0 {
					return expr[:eq+1]
				}
			}
			return expr[:i]
		}
	}
	return expr
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

func escapeRegex(s string) string {
	s = escape(s)
	if strings.Contains(s, "|") {
		s = "(" + s + ")"
	}
	return s
}

func likeEscape(s string) string {
	s = strings.ReplaceAll(s, `_`, `\_`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

func quote(value interface{}) string {
	switch v := value.(type) {
	case int:
		return fmt.Sprintf("%#v", v)
	case uint32:
		return fmt.Sprintf("%#v", v)
	case string:
		return fmt.Sprintf("'%s'", escape(v))
	case []byte:
		return fmt.Sprintf("'%s'", escape(unsafeString(v)))
	default:
		panic("not implemented")
	}
}

func quoteRegex(key, value string) string {
	startLine := value[0] == '^'
	if startLine {
		return fmt.Sprintf("'^%s%s%s'", key, opEq, escapeRegex(value[1:]))
	}
	return fmt.Sprintf("'^%s%s.*%s'", key, opEq, escapeRegex(value))
}

func Like(field, s string) string {
	return fmt.Sprintf("%s LIKE '%s'", field, s)
}

func Eq(field, value interface{}) string {
	return fmt.Sprintf("%s=%s", field, quote(value))
}

func HasPrefix(field, prefix string) string {
	return fmt.Sprintf("%s LIKE '%s%%'", field, likeEscape(prefix))
}

func HasPrefixAndNotEq(field, prefix string) string {
	return fmt.Sprintf("%s LIKE '%s_%%'", field, likeEscape(prefix))
}

func HasPrefixBytes(field, prefix []byte) string {
	return fmt.Sprintf("%s LIKE '%s%%'", field, likeEscape(unsafeString(prefix)))
}

func ArrayHas(field, element string) string {
	return fmt.Sprintf("has(%s, %s)", field, quote(element))
}

func In(field string, list []string) string {
	if len(list) == 1 {
		return Eq(field, list[0])
	}

	var buf strings.Builder
	buf.WriteString(field)
	buf.WriteString(" IN (")
	for i, v := range list {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(quote(v))
	}
	buf.WriteByte(')')
	return buf.String()
}

func InTable(field string, table string) string {
	return fmt.Sprintf("%s in %s", field, table)
}

func DateBetween(field string, from int64, until int64) string {
	return fmt.Sprintf(
		"%s >= '%s' AND %s <= '%s'",
		field, date.FromTimestampToDaysFormat(from), field, date.UntilTimestampToDaysFormat(until),
	)
}

func TimestampBetween(field string, from int64, until int64) string {
	return fmt.Sprintf("%s >= %d AND %s <= %d", field, from, field, until)
}

type Where struct {
	where string
}

func New() *Where {
	return &Where{}
}

func (w *Where) And(exp string) {
	if exp == "" {
		return
	}
	if w.where != "" {
		w.where = fmt.Sprintf("(%s) AND (%s)", w.where, exp)
	} else {
		w.where = exp
	}
}

func (w *Where) Or(exp string) {
	if exp == "" {
		return
	}
	if w.where != "" {
		w.where = fmt.Sprintf("(%s) OR (%s)", w.where, exp)
	} else {
		w.where = exp
	}
}

func (w *Where) Andf(format string, obj ...interface{}) {
	w.And(fmt.Sprintf(format, obj...))
}

func (w *Where) String() string {
	return w.where
}

func (w *Where) SQL() string {
	if w.where == "" {
		return ""
	}
	return "WHERE " + w.where
}

func (w *Where) PreWhereSQL() string {
	if w.where == "" {
		return ""
	}
	return "PREWHERE " + w.where
}
