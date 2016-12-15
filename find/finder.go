package find

import (
	"regexp"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
)

type Finder struct {
	config        *config.Config
	query         string // original query
	prefix        string // prefix from config
	q             string // query after remove prefix, convert from glob to regexp
	prefixReply   string // single reply
	prefixMatched bool   //
}

func NewFinder(query string, config *config.Config) (*Finder, error) {
	f := &Finder{
		query:  query,
		config: config,
		prefix: config.ClickHouse.ExtraPrefix,
	}

	if err := f.prepare(); err != nil {
		return nil, err
	}

	return f, nil
}

func (f *Finder) prepare() error {
	qs := strings.Split(f.query, ".")

	// check regexp
	for _, queryPart := range qs {
		if _, err := regexp.Compile(GlobToRegexp(queryPart)); err != nil {
			return err
		}
	}

	ps := make([]string, 0)
	if f.prefix != "" {
		ps = strings.Split(f.prefix, ".")
	}

	var i int
	for i = 0; i < len(qs) && i < len(ps); i++ {
		m, err := regexp.MatchString(GlobToRegexp(qs[i]), ps[i])
		if err != nil {
			return err
		}
		if !m { // not matched
			return nil
		}
	}

	f.prefixMatched = true

	if len(qs) <= len(ps) {
		// prefix matched, but not finished
		f.prefixReply = strings.Join(ps[:len(qs)], ".") + "."
	} else {
		f.q = strings.Join(qs[len(ps):], ".")
	}

	return nil
}

func (f *Finder) Execute() ([][]byte, error) {
	// check prefix
	// p, q, RemoveExtraPrefix

	return nil, nil
}
