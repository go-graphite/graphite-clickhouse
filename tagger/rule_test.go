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
regexp = "reg[e]xp"
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
		{"prefix.metric", "suffix", nil},
		{"prefix.metric", "contains", nil},
		{"prefix.metric", "other", nil},

		{"metric.suffix", "", []string{"suffix"}},
		{"metric.suffix", "prefix", nil},
		{"metric.suffix", "suffix", []string{"suffix"}},
		{"metric.suffix", "contains", nil},
		{"metric.suffix", "other", nil},

		{"hello.contains.world", "", []string{"contains"}},
		{"hello.contains.world", "prefix", nil},
		{"hello.contains.world", "suffix", nil},
		{"hello.contains.world", "contains", []string{"contains"}},
		{"hello.contains.world", "other", nil},

		{"hello.regexp.world", "", []string{"regexp"}},
		{"hello.regexp.world", "prefix", nil},
		{"hello.regexp.world", "suffix", nil},
		{"hello.regexp.world", "contains", nil},
		{"hello.regexp.world", "other", []string{"regexp"}},
	}

	for i := 0; i < len(table); i++ {
		t := table[i]

		m := Metric{Path: []byte(t.path), Tags: EmptySet}

		switch t.method {
		case "":
			rules.Match(&m)
		case "prefix":
			rules.matchPrefix(&m)
		case "suffix":
			rules.matchSuffix(&m)
		case "contains":
			rules.matchContains(&m)
		case "other":
			rules.matchOther(&m)
		}

		expected := t.expectedTags
		if expected == nil {
			expected = []string{}
		}
		sort.Strings(expected)
		tags := m.Tags.List()
		sort.Strings(tags)

		assert.Equal(expected, tags, fmt.Sprintf("path: %s, method: %s", t.path, t.method))
	}
}
