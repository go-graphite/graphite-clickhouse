package where

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unsafe"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
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

func IndexReverseWildcard(target string) int {
	return strings.LastIndexAny(target, "[]{}*?")
}

func IndexWildcardOrDot(target string) int {
	return strings.IndexAny(target, ".[]{}*?")
}

func NonRegexpPrefix(expr string) string {
	s := regexp.QuoteMeta(expr)
	for i := 0; i < len(expr); i++ {
		if expr[i] != s[i] || expr[i] == '\\' {
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

func DateBetween(field string, from time.Time, until time.Time) string {
	return fmt.Sprintf(
		"%s >='%s' AND %s <= '%s'",
		field, from.Format("2006-01-02"), field, until.Format("2006-01-02"),
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
