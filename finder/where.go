package finder

import (
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func GlobToRegexp(g string) string {
	s := g
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
