package find

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type Finder struct {
	config          *config.Config
	context         context.Context
	query           string // original query
	prefix          string // prefix from config
	effectivePrefix string // real prefix for add to response
	q               string // query after remove prefix, convert from glob to regexp
	prefixReply     string // single reply
	prefixMatched   bool   //
	body            []byte // raw clickhouse response
}

func NewFinder(query string, config *config.Config, ctx context.Context) (*Finder, error) {
	f := &Finder{
		query:   query,
		config:  config,
		prefix:  config.ClickHouse.ExtraPrefix,
		context: ctx,
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
		return nil
	}

	qs = qs[len(ps):]
	f.q = strings.Join(qs, ".")
	f.effectivePrefix = f.prefix

	// TAGS
	// qs = strings.Split(f.query, ".")
	// if qs[0] == "_tag" {
	// }

	return nil
}

func (f *Finder) Execute() error {
	if !f.prefixMatched {
		return nil
	}

	if f.prefixReply != "" {
		f.body = []byte(f.prefixReply)
		return nil
	}

	where := MakeWhere(f.q, true)

	var err error
	f.body, err = clickhouse.Query(
		f.context,
		f.config.ClickHouse.Url,
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", f.config.ClickHouse.TreeTable, where),
		f.config.ClickHouse.TreeTimeout.Value(),
	)

	return err
}

// add prefix and remove last dot
func (f *Finder) Path(path string) string {
	if f.effectivePrefix != "" {
		if path == "" {
			return f.effectivePrefix
		} else {
			if path[len(path)-1] == '.' {
				return f.effectivePrefix + "." + path[:len(path)-1]
			} else {
				return f.effectivePrefix + "." + path
			}
		}
	}

	if len(path) > 0 && path[len(path)-1] == '.' {
		return path[:len(path)-1]
	}

	return path
}

// check last byte
func (f *Finder) IsLeaf(path string) bool {
	if path == "" {
		return false
	}
	return path[len(path)-1] != '.'
}
