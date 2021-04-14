package finder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaggedWhere(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query    string
		where    string
		prewhere string
		isErr    bool
	}{
		// info about _tag "directory"
		{"seriesByTag('key=value')", "Tag1='key=value'", "", false},
		{"seriesByTag('name=rps')", "Tag1='__name__=rps'", "", false},
		{"seriesByTag('name=~cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=cpu%' AND match(Tag1, '^__name__=cpu.usage$')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=cpu%' AND match(Tag1, '^__name__=cpu.usage$')", false},
		{"seriesByTag('name=~cpu|mem')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=cpu|mem$')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=cpu|mem$')", false},
		{"seriesByTag('name=rps', 'key=~value')", "(Tag1='__name__=rps') AND (arrayExists((x) -> x='key=value', Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~hello.world')", "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=hello%' AND match(x, '^key=hello.world$'), Tags))", "", false},
		{`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, `(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'host=Vladimirs-MacBook-Pro%' AND match(x, '^host=Vladimirs-MacBook-Pro\\.local$'), Tags))`, "", false},
		// grafana multi-value variable produce this
		{"seriesByTag('name=value','what=*')", "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},        // If All masked to value with *
		{"seriesByTag('name=value','what=*x')", "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false},      // If All masked to value with *
		{"seriesByTag('name=value','what!=*x')", "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		{"seriesByTag('name={avg,max}')", "Tag1 IN ('__name__=avg','__name__=max')", "", false},
		{"seriesByTag('name=m{in,ax}')", "Tag1 IN ('__name__=min','__name__=max')", "", false},
		{"seriesByTag('name=m{in,ax')", "Tag1='__name__=m{in,ax'", "", true},
		{"seriesByTag('name=value','what={avg,max}')", "(Tag1='__name__=value') AND (arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		{"seriesByTag('name=value','what!={avg,max}')", "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		terms, err := ParseSeriesByTag(test.query)

		if !test.isErr {
			assert.NoError(err, testName+", err")
		}

		w, pw, err := TaggedWhere(terms)

		if test.isErr {
			assert.Error(err, testName+", err")
			continue
		} else {
			assert.NoError(err, testName+", err")
		}

		assert.Equal(test.where, w.String(), testName+", where")
		assert.Equal(test.prewhere, pw.String(), testName+", prewhere")
	}
}

func TestParseSeriesByTag(t *testing.T) {
	assert := assert.New(t)

	ok := func(query string, expected []TaggedTerm) {
		p, err := ParseSeriesByTag(query)
		assert.NoError(err)
		assert.Equal(expected, p)
	}

	ok(`seriesByTag('key=value')`, []TaggedTerm{
		TaggedTerm{Op: TaggedTermEq, Key: "key", Value: "value"},
	})

	ok(`seriesByTag('name=rps')`, []TaggedTerm{
		TaggedTerm{Op: TaggedTermEq, Key: "__name__", Value: "rps"},
	})

	ok(`seriesByTag('name=~cpu.usage')`, []TaggedTerm{
		TaggedTerm{Op: TaggedTermMatch, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('name!=cpu.usage')`, []TaggedTerm{
		TaggedTerm{Op: TaggedTermNe, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('name!=~cpu.usage')`, []TaggedTerm{
		TaggedTerm{Op: TaggedTermNotMatch, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, []TaggedTerm{
		TaggedTerm{Op: TaggedTermEq, Key: "cpu", Value: "cpu-total"},
		TaggedTerm{Op: TaggedTermMatch, Key: "host", Value: `Vladimirs-MacBook-Pro\.local`},
	})

}
