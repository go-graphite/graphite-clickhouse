package tagger

import (
	"bytes"
	"regexp"
)

type Tag struct {
	Name           string         `toml:"name"`
	List           []string       `toml:"list"`
	re             *regexp.Regexp `toml:"-"`
	Equal          string         `toml:"equal"`
	HasPrefix      string         `toml:"has-prefix"`
	HasSuffix      string         `toml:"has-suffix"`
	Contains       string         `toml:"contains"`
	Regexp         string         `toml:"regexp"`
	BytesEqual     []byte         `toml:"-"`
	BytesHasPrefix []byte         `toml:"-"`
	BytesHasSuffix []byte         `toml:"-"`
	BytesContains  []byte         `toml:"-"`
}

type Rules struct {
	Tag []Tag `toml:"tag"`
}

func (r *Tag) MatchAndMark(m *Metric) {
	if r.BytesEqual != nil && !bytes.Equal(m.Path, r.BytesEqual) {
		return
	}

	if r.BytesHasPrefix != nil && !bytes.HasPrefix(m.Path, r.BytesHasPrefix) {
		return
	}

	if r.BytesHasSuffix != nil && !bytes.HasSuffix(m.Path, r.BytesHasSuffix) {
		return
	}

	if r.BytesContains != nil && !bytes.Contains(m.Path, r.BytesContains) {
		return
	}

	if r.re != nil && !r.re.Match(m.Path) {
		return
	}

	if r.Name != "" {
		m.Tags[r.Name] = true
	}

	if r.List != nil {
		for _, n := range r.List {
			m.Tags[n] = true
		}
	}
}
