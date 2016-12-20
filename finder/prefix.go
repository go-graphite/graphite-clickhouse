package finder

import (
	"regexp"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type PrefixMatchResult int

const (
	PrefixNotMatched PrefixMatchResult = iota
	PrefixMatched
	PrefixPartialMathed
)

type PrefixFinder struct {
	wrapped     Finder
	config      *config.Config    // config
	logger      *zap.Logger       // config
	prefix      string            // config
	matched     PrefixMatchResult // request
	matchedPart string            // request. for partial matched
}

func WrapPrefix(f Finder, prefix string, config *config.Config, logger *zap.Logger) *PrefixFinder {
	return &PrefixFinder{
		wrapped: f,
		prefix:  prefix,
		logger:  logger,
		config:  config,
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
