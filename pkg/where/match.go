package where

import (
	"fmt"
	"strings"
)

var (
	opEq    string = "="
	opMatch string = "=~"
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
		return HasPrefix(field, simplePrefix)
	}

	// Q() replaces \ with \\, so using \. does not work here.
	// work around with [.]
	postfix := `$`
	if optionalDotAtEnd {
		postfix = `[.]?$`
	}

	if simplePrefix == "" {
		return fmt.Sprintf("match(%s, %s)", field, quote(`^`+GlobToRegexp(query)+postfix))
	}

	return fmt.Sprintf("%s AND match(%s, %s)",
		HasPrefix(field, simplePrefix),
		field, quote(`^`+GlobToRegexp(query)+postfix),
	)
}

// Glob ...
func Glob(field string, query string) string {
	return glob(field, query, false)
}

// TreeGlob ...
func TreeGlob(field string, query string) string {
	return glob(field, query, true)
}

func ConcatMatchKV(key, value string) string {
	startLine := value[0] == '^'
	endLine := value[len(value)-1] == '$'
	if startLine {
		return key + opEq + value[1:]
	} else if endLine {
		return key + opEq + value + "\\\\%"
	}
	return key + opEq + "\\\\%" + value
}

func Match(field string, key, value string) string {
	expr := ConcatMatchKV(key, value)
	simplePrefix := NonRegexpPrefix(expr)
	if len(simplePrefix) == len(expr) {
		return Eq(field, expr)
	} else if len(simplePrefix) == len(expr)-1 && expr[len(expr)-1] == '$' {
		return Eq(field, simplePrefix)
	}

	if simplePrefix == "" {
		return fmt.Sprintf("match(%s, %s)", field, quoteRegex(key, value))
	}

	return fmt.Sprintf("%s AND match(%s, %s)",
		HasPrefix(field, simplePrefix),
		field, quoteRegex(key, value),
	)
}
