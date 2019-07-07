package where

import (
	"fmt"
	"strings"
)

func glob(field string, query string, optionalDotAtEnd bool) string {
	if query == "*" {
		return ""
	}

	if !HasWildcard(query) {
		if optionalDotAtEnd {
			return In(field, []string{query, query + "."})
		} else {
			return Eq(field, query)
		}
	}

	w := New()

	// before any wildcard symbol
	simplePrefix := query[:strings.IndexAny(query, "[]{}*?")]

	if len(simplePrefix) > 0 {
		w.And(HasPrefix(field, simplePrefix))
	}

	// prefix search like "metric.name.xx*"
	if len(simplePrefix) == len(query)-1 && query[len(query)-1] == '*' {
		return w.String()
	}

	// Q() replaces \ with \\, so using \. does not work here.
	// work around with [.]
	postfix := `$`
	if optionalDotAtEnd {
		postfix = `[.]?$`
	}
	w.Andf("match(%s, %s)", field, quote(`^`+GlobToRegexp(query)+postfix))
	return w.String()
}

// Glob ...
func Glob(field string, query string) string {
	return glob(field, query, false)
}

// TreeGlob ...
func TreeGlob(field string, query string) string {
	return glob(field, query, true)
}

func Match(field string, expr string) string {
	simplePrefix := NonRegexpPrefix(expr)
	if len(simplePrefix) == len(expr) {
		return Eq(field, expr)
	}

	if simplePrefix == "" {
		return fmt.Sprintf("match(%s, %s)", field, quote(expr))
	}

	return fmt.Sprintf("(%s) AND (match(%s, %s))", HasPrefix(field, simplePrefix), field, quote(expr))
}
