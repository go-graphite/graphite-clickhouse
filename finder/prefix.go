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
	matched     PrefixMatchResult // request
	matchedPart string            // request. for partial matched
}

func WrapPrefix(f Finder, prefix string) *PrefixFinder {
	return &PrefixFinder{
		wrapped: f,
		prefix:  prefix,
	}
}

func (p *PrefixFinder) Execute(query string) error {
	return nil
}

func (p *PrefixFinder) List() [][]byte {
	return nil
}

func (p *PrefixFinder) Abs([]byte) []byte {
	return nil
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
