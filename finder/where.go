package finder

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func GlobToRegexp(g string) string {
	s := g
	s = strings.Replace(s, ".", "[.]", -1)
	s = strings.Replace(s, "{", "(", -1)
	s = strings.Replace(s, "}", ")", -1)
	s = strings.Replace(s, "?", "[^.]", -1)
	s = strings.Replace(s, ",", "|", -1)
	s = strings.Replace(s, "*", "([^.]*?)", -1)
	return s
}

func HasWildcard(target string) bool {
	return strings.IndexAny(target, "[]{}*?") > -1
}

func NonRegexpPrefix(expr string) string {
	s := regexp.QuoteMeta(expr)
	for i := 0; i < len(expr); i++ {
		if expr[i] != s[i] {
			return expr[:i]
		}
	}
	return expr
}

func likeEscape(v string) string {
	return strings.Replace(v, "_", "\\_", -1)
}

// Q quotes string for clickhouse
func Q(v string) string {
	return "'" + clickhouse.Escape(v) + "'"
}

func Qf(format string, obj ...interface{}) string {
	return Q(fmt.Sprintf(format, obj...))
}

type Where struct {
	where string
}

func NewWhere() *Where {
	return &Where{}
}

func (w *Where) And(exp string) {
	if exp == "" {
		return
	}
	if w.where != "" {
		w.where = fmt.Sprintf("%s AND (%s)", w.where, exp)
	} else {
		w.where = fmt.Sprintf("(%s)", exp)
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
