package tagger

import (
	"bytes"
	"io/ioutil"
	"regexp"

	"github.com/BurntSushi/toml"
)

type Rule struct {
	Single         string         `toml:"tag"`
	List           []string       `toml:"tags"`
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
	Tags           *Set           `toml:"-"`
}

type Rules struct {
	Rule     []Rule  `toml:"rule"`
	prefix   *Tree   `toml:"-"`
	suffix   *Tree   `toml:"-"` // @TODO
	contains *Tree   `toml:"-"`
	other    []*Rule `toml:"-"`
}

func ParseFile(filename string) (*Rules, error) {
	c, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return Parse(string(c))
}

func Parse(content string) (*Rules, error) {
	rules := &Rules{
		prefix:   &Tree{},
		suffix:   &Tree{},
		contains: &Tree{},
		other:    make([]*Rule, 0),
	}

	if _, err := toml.Decode(content, rules); err != nil {
		return nil, err
	}

	var err error

	for i := 0; i < len(rules.Rule); i++ {
		rule := &rules.Rule[i]
		rule.Tags = EmptySet

		if rule.Single != "" {
			rule.Tags = rule.Tags.Add(rule.Single)
		}

		if rule.List != nil {
			rule.Tags = rule.Tags.Add(rule.List...)
		}

		// compile and check regexp
		rule.re, err = regexp.Compile(rule.Regexp)
		if err != nil {
			return nil, err
		}
		if rule.Equal != "" {
			rule.BytesEqual = []byte(rule.Equal)
		}
		if rule.Contains != "" {
			rule.BytesContains = []byte(rule.Contains)
		}
		if rule.HasPrefix != "" {
			rule.BytesHasPrefix = []byte(rule.HasPrefix)
		}
		if rule.HasSuffix != "" {
			rule.BytesHasSuffix = []byte(rule.HasSuffix)
		}

		if rule.BytesHasPrefix != nil {
			rules.prefix.Add(rule.BytesHasPrefix, rule)
		} else if rule.BytesEqual != nil {
			rules.prefix.Add(rule.BytesEqual, rule)
		} else if rule.BytesContains != nil {
			rules.contains.Add(rule.BytesContains, rule)
		} else if rule.BytesHasSuffix != nil {
			rules.suffix.AddSuffix(rule.BytesHasSuffix, rule)
		} else {
			rules.other = append(rules.other, rule)
		}
	}

	return rules, nil
}

func (r *Rules) Match(m *Metric) {
	r.matchPrefix(m)
	r.matchSuffix(m)
	r.matchContains(m)
	r.matchOther(m)
}

func matchByPrefix(path []byte, tree *Tree, m *Metric) {
	x := tree
	i := 0
	for {
		if i >= len(path) {
			break
		}

		x = x.Next[path[i]]
		if x == nil {
			break
		}

		if x.Rules != nil {
			for _, rule := range x.Rules {
				rule.Match(m)
			}
		}

		i++
	}
}

func matchBySuffix(path []byte, tree *Tree, m *Metric) {
	x := tree
	i := len(path) - 1
	for {
		if i <= 0 {
			break
		}

		x = x.Next[path[i]]
		if x == nil {
			break
		}

		if x.Rules != nil {
			for _, rule := range x.Rules {
				rule.Match(m)
			}
		}

		i--
	}
}

func (r *Rules) matchPrefix(m *Metric) {
	matchByPrefix(m.Path, r.prefix, m)
}

func (r *Rules) matchSuffix(m *Metric) {
	matchBySuffix(m.Path, r.suffix, m)
}

func (r *Rules) matchContains(m *Metric) {
	for i := 0; i < len(m.Path); i++ {
		matchByPrefix(m.Path[i:], r.contains, m)
	}
}

func (r *Rules) matchOther(m *Metric) {
	for _, rule := range r.other {
		rule.Match(m)
	}
}

func (r *Rule) Match(m *Metric) {
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

	m.Tags = m.Tags.Merge(r.Tags)
}
