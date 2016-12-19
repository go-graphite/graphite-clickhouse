package find

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func HasWildcard(target string) bool {
	return strings.IndexAny(target, "[]{}*") > -1
}

func MakeWhere(target string, withLevel bool) (where string) {
	level := strings.Count(target, ".") + 1

	AND := func(exp string) {
		if where == "" {
			where = exp
		} else {
			where = fmt.Sprintf("(%s) AND %s", where, exp)
		}
	}

	if withLevel {
		where = fmt.Sprintf("Level = %d", level)
	}

	if target == "*" {
		return
	}

	// simple metric
	if !HasWildcard(target) {
		AND(fmt.Sprintf("Path = '%s' OR Path = '%s.'", clickhouse.Escape(target), clickhouse.Escape(target)))
		return
	}

	// before any wildcard symbol
	simplePrefix := target[:strings.IndexAny(target, "[]{}*")]

	if len(simplePrefix) > 0 {
		AND(fmt.Sprintf("Path LIKE '%s%%'", clickhouse.Escape(simplePrefix)))
	}

	// prefix search like "metric.name.xx*"
	if len(simplePrefix) == len(target)-1 && target[len(target)-1] == '*' {
		return
	}

	pattern := GlobToRegexp(target)
	AND(fmt.Sprintf("match(Path, '^%s$')", clickhouse.Escape(pattern)))

	return
}

func GlobToRegexp(g string) string {
	s := g
	s = strings.Replace(s, "*", "([^.]*?)", -1)
	s = strings.Replace(s, "{", "(", -1)
	s = strings.Replace(s, "}", ")", -1)
	s = strings.Replace(s, ",", "|", -1)
	return s
}

func RemoveExtraPrefix(prefix, query string) (string, string, error) {
	qs := strings.Split(query, ".")
	ps := strings.Split(prefix, ".")

	var i int
	for i = 0; i < len(qs) && i < len(ps); i++ {
		m, err := regexp.MatchString(GlobToRegexp(qs[i]), ps[i])
		if err != nil {
			return "", "", err
		}
		if !m { // not matched
			return "", "", nil
		}
	}

	if i < len(ps) {
		return strings.Join(ps[:i], "."), "", nil
	}

	return prefix, strings.Join(qs[i:], "."), nil
}
