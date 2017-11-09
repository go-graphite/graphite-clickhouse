package finder

import (
	"fmt"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func GlobToRegexp(g string) string {
	s := g
	s = strings.Replace(s, ".", "[.]", -1)
	s = strings.Replace(s, "*", "([^.]*?)", -1)
	s = strings.Replace(s, "{", "(", -1)
	s = strings.Replace(s, "}", ")", -1)
	s = strings.Replace(s, ",", "|", -1)
	return s
}

func HasWildcard(target string) bool {
	return strings.IndexAny(target, "[]{}*") > -1
}

// Q quotes string for clickhouse
func Q(v string) string {
	return "'" + clickhouse.Escape(v) + "'"
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
