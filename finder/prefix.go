package finder

import (
	"regexp"
	"strings"
)

type PrefixMatchResult int

const (
	PrefixNotMatched PrefixMatchResult = iota
	PrefixMatched
	PrefixPartialMathed
)

type PrefixFinder struct {
	wrapped     Finder
	prefix      string            // config
	prefixBytes []byte            // same prefix with []bytes type
	matched     PrefixMatchResult // request
	part        string            // request. partially matched part
}

func WrapPrefix(f Finder, prefix string) *PrefixFinder {
	return &PrefixFinder{
		wrapped:     f,
		prefix:      prefix,
		prefixBytes: append([]byte(prefix), '.'),
		matched:     PrefixNotMatched,
	}
}

func (p *PrefixFinder) Execute(query string) error {
	qs := strings.Split(query, ".")

	// check regexp
	for _, queryPart := range qs {
		if _, err := regexp.Compile(GlobToRegexp(queryPart)); err != nil {
			return err
		}
	}

	ps := strings.Split(p.prefix, ".")

	var i int
	for i = 0; i < len(qs) && i < len(ps); i++ {
		m, err := regexp.MatchString("^"+GlobToRegexp(qs[i])+"$", ps[i])
		if err != nil {
			return err
		}
		if !m { // not matched
			return nil
		}
	}

	if len(qs) <= len(ps) {
		// prefix matched, but not finished
		p.part = strings.Join(ps[:len(qs)], ".") + "."
		p.matched = PrefixPartialMathed
		return nil
	}

	p.matched = PrefixMatched

	return p.wrapped.Execute(strings.Join(qs[len(ps):], "."))
}

func (p *PrefixFinder) List() [][]byte {
	if p.matched == PrefixNotMatched {
		return [][]byte{}
	}

	if p.matched == PrefixPartialMathed {
		return [][]byte{[]byte(p.part)}
	}

	list := p.wrapped.List()
	result := make([][]byte, len(list))

	for i := 0; i < len(list); i++ {
		result[i] = append(p.prefixBytes, list[i]...)
	}

	return result
}

// For Render
func (p *PrefixFinder) Series() [][]byte {
	if p.matched == PrefixNotMatched {
		return [][]byte{}
	}

	if p.matched != PrefixMatched {
		return [][]byte{}
	}

	return p.wrapped.Series()
}

func (p *PrefixFinder) Abs(value []byte) ([]byte, bool) {
	// @TODO: call wrapped
	return append(p.prefixBytes, value...), false
}
