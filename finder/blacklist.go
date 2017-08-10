package finder

import "regexp"

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

func (p *BlacklistFinder) Execute(query string) error {
	for i := 0; i < len(p.blacklist); i++ {
		if p.blacklist[i].MatchString(query) {
			p.matched = true
			return nil
		}
	}

	return p.wrapped.Execute(query)
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

func (p *BlacklistFinder) Abs(value []byte) []byte {
	return p.wrapped.Abs(value)
}
