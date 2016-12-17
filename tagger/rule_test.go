package tagger

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

var RulesConf = `
[[tag]]
name = "prefix"
has-prefix = "prefix"

[[tag]]
name = "suffix"
has-suffix = "suffix"

[[tag]]
name = "contains"
contains = "contains"

[[tag]]
name = "equal"
equal = "equal"

[[tag]]
name = "regexp"
regexp = "regexp"
`

func TestRules(t *testing.T) {
	assert := assert.New(t)
	rules, err := Parse(RulesConf)

	assert.NoError(err)

	table := []struct {
		path         string
		method       string // "" for all, "prefix", "suffix", "contains" for use only specified tree
		expectedTags []string
	}{
		{"prefix.metric", "", []string{"prefix"}},
		{"prefix.metric", "prefix", []string{"prefix"}},
		{"prefix.metric", "suffix", []string{}},
		{"prefix.metric", "contains", []string{}},
		{"prefix.metric", "other", []string{}},
	}

	for i := 0; i < len(table); i++ {
		t := table[i]

		m := Metric{Path: []byte(t.path), Tags: EmptySet}

		switch t.method {
		case "":
			rules.Match(&m)
		case "prefix":
			rules.matchPrefix(&m)
		case "contains":
			rules.matchContains(&m)
		case "other":
			rules.matchOther(&m)
		}

		sort.Strings(t.expectedTags)
		tags := m.Tags.List()
		sort.Strings(tags)

		assert.Equal(tags, t.expectedTags, fmt.Sprintf("path: %s, method: %s", t.path, t.method))
	}
}
