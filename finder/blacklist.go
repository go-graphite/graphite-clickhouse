package finder

import (
	"context"
	"regexp"

	"github.com/lomik/graphite-clickhouse/config"
)

type BlacklistFinder struct {
	wrapped   Finder
	blacklist []*regexp.Regexp // config
	matched   bool
}

func WrapBlacklist(f Finder, blacklist []*regexp.Regexp) *BlacklistFinder {
	return &BlacklistFinder{
		wrapped:   f,
		blacklist: blacklist,
	}
}

func (p *BlacklistFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	for i := 0; i < len(p.blacklist); i++ {
		if p.blacklist[i].MatchString(query) {
			p.matched = true
			return
		}
	}

	return p.wrapped.Execute(ctx, config, query, from, until, stat)
}

func (p *BlacklistFinder) List() [][]byte {
	if p.matched {
		return [][]byte{}
	}

	return p.wrapped.List()
}

// For Render
func (p *BlacklistFinder) Series() [][]byte {
	if p.matched {
		return [][]byte{}
	}

	return p.wrapped.Series()
}

func (p *BlacklistFinder) Abs(v []byte) []byte {
	return p.wrapped.Abs(v)
}

func (p *BlacklistFinder) Bytes() ([]byte, error) {
	return nil, ErrNotImplemented
}
