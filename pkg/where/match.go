package where

import (
	"fmt"
	"strings"
)

var (
	opEq string = "="
)

// clearGlob cleanup grafana globs like {name}
func clearGlob(query string) string {
	p := 0
	s := strings.IndexAny(query, "{[")
	if s == -1 {
		return query
	}

	found := false
	var builder strings.Builder

	for {
		var e int
		if query[s] == '{' {
			e = strings.IndexAny(query[s:], "}.")
			if e == -1 || query[s+e] == '.' {
				// { not closed, glob with error
				break
			}
			e += s + 1
			delim := strings.IndexRune(query[s+1:e], ',')
			if delim == -1 {
				if !found {
					builder.Grow(len(query) - 2)
					found = true
				}
				builder.WriteString(query[p:s])
				builder.WriteString(query[s+1 : e-1])
				p = e
			}
		} else {
			e = strings.IndexAny(query[s+1:], "].")
			if e == -1 || query[s+e] == '.' {
				// [ not closed, glob with error
				break
			} else {
				symbols := 0
				for _, c := range query[s+1 : s+e+1] {
					_ = c // for loop over runes
					symbols++
					if symbols == 2 {
						break
					}
				}
				if symbols <= 1 {
					if !found {
						builder.Grow(len(query) - 2)
						found = true
					}
					builder.WriteString(query[p:s])
					builder.WriteString(query[s+1 : s+e+1])
					p = e + s + 2
				}
			}
			e += s + 2
		}

		if e >= len(query) {
			break
		}
		s = strings.IndexAny(query[e:], "{[")
		if s == -1 {
			break
		}
		s += e
	}

	if found {
		if p < len(query) {
			builder.WriteString(query[p:])
		}
		return builder.String()
	}
	return query
}

func glob(field string, query string, optionalDotAtEnd bool) string {
	if query == "*" {
		return ""
	}

	query = clearGlob(query)

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
	if startLine && endLine {
		return key + opEq + value[1:]
	} else if startLine {
		return key + opEq + value[1:] + "\\\\%"
	}
	return key + opEq + "\\\\%" + value
}

func Match(field string, key, value string) string {
	if len(value) == 0 || value == "*" {
		return Like(field, key+"=%")
	}
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
