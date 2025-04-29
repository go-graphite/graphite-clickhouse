package finder

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	chtest "github.com/lomik/graphite-clickhouse/helper/tests/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaggedWhere(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	table := []struct {
		query    string
		minTags  int
		where    string
		prewhere string
		isErr    bool
	}{
		// test for issue #195
		{"seriesByTag()", 0, "", "", true},
		{"seriesByTag('')", 0, "", "", true},
		// incomplete
		{"seriesByTag('key=value)", 0, "", "", true},
		// missing quote
		{"seriesByTag(key=value)", 0, "", "", true},
		// info about _tag "directory"
		{"seriesByTag('key=value')", 0, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 1, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 2, "", "", true},
		// test case for wildcarded name, must be not first check
		{"seriesByTag('name=*', 'key=value')", 0, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 1, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 2, "", "", true},
		{"seriesByTag('name=*', 'key=value*')", 0, "(Tag1 LIKE '__name__=%') AND (arrayExists((x) -> x LIKE 'key=value%', Tags))", "", false},
		{"seriesByTag('name=rps')", 0, "Tag1='__name__=rps'", "", false},
		{"seriesByTag('name=~cpu.usage')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", false},
		{"seriesByTag('name=~cpu.usage')", 1, "", "", true},
		{"seriesByTag('name=~cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", false},
		{"seriesByTag('name=~cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", false},
		{"seriesByTag('name=~^cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", false},
		{"seriesByTag('name=~^cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", false},
		{"seriesByTag('name=rps', 'key=~value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*value'), Tags))", "", false},
		// test for issue #244
		{"seriesByTag('name=rps', 'key=~^value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value.*')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value.*'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^valu[a-e]')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=valu%' AND match(x, '^key=valu[a-e]'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value$')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x='key=value', Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~hello.world')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*hello.world'), Tags))", "", false},
		{`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, 0, `(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'host=%' AND match(x, '^host=.*Vladimirs-MacBook-Pro\\.local'), Tags))`, "", false},
		// grafana multi-value variable produce this
		{"seriesByTag('name=value','what=*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},        // If All masked to value with *
		{"seriesByTag('name=value','what=*x')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false},      // If All masked to value with *
		{"seriesByTag('name=value','what!=*x')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		{"seriesByTag('name={avg,max}')", 0, "Tag1 IN ('__name__=avg','__name__=max')", "", false},
		{"seriesByTag('name=m{in}')", 0, "Tag1='__name__=min'", "", false},
		{"seriesByTag('name=m{in,ax}')", 0, "Tag1 IN ('__name__=min','__name__=max')", "", false},
		{"seriesByTag('name=m{in,ax')", 0, "", "", true},
		{"seriesByTag('name=value','what={avg,max}')", 0, "(Tag1='__name__=value') AND ((has(Tags, 'what=avg')) OR (has(Tags, 'what=max')))", "", false},
		{"seriesByTag('name=value','what!={avg,max}')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		// grafana workaround for multi-value variables default, masked with *
		{"seriesByTag('name=value','what=~*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// empty tag value during autocompletion
		{"seriesByTag('name=value','what=~')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// testcases for useCarbonBehaviour=false
		{"seriesByTag('what=')", 0, "Tag1='what='", "", false},
		{"seriesByTag('name=value','what=')", 0, "(Tag1='__name__=value') AND (has(Tags, 'what='))", "", false},
		// testcases for dontMatchMissingTags=false
		{"seriesByTag('key!=value')", 0, "NOT arrayExists((x) -> x='key=value', Tags)", "", false},
		{"seriesByTag('dc!=de', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (NOT arrayExists((x) -> x='dc=de', Tags))", "", false},
		{"seriesByTag('dc!=de', 'cpu!=cpu-total')", 0, "(NOT arrayExists((x) -> x='dc=de', Tags)) AND (NOT arrayExists((x) -> x='cpu=cpu-total', Tags))", "", false},
		{"seriesByTag('dc!=~de|us', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (NOT arrayExists((x) -> x LIKE 'dc=%' AND match(x, '^dc=.*(de|us)'), Tags))", "", false},
	}

	for i, test := range table {
		t.Run(test.query+"#"+strconv.Itoa(i), func(t *testing.T) {
			testName := fmt.Sprintf("query: %#v", test.query)

			config := config.New()
			config.ClickHouse.TagsMinInQuery = test.minTags
			terms, err := ParseSeriesByTag(test.query, config)
			sort.Sort(TaggedTermList(terms))

			if test.isErr {
				if err != nil {
					return
				}
			}
			require.NoError(err, testName+", err")

			var w, pw *where.Where
			if err == nil {
				w, pw, err = TaggedWhere(terms, false, false)
			}

			if test.isErr {
				require.Error(err, testName+", err")
				return
			} else {
				assert.NoError(err, testName+", err")
			}

			assert.Equal(test.where, w.String(), testName+", where")
			assert.Equal(test.prewhere, pw.String(), testName+", prewhere")
		})
	}
}

func TestTaggedWhere_UseCarbonBehaviourFlag(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	table := []struct {
		query    string
		minTags  int
		where    string
		prewhere string
		isErr    bool
	}{
		// test for issue #195
		{"seriesByTag()", 0, "", "", true},
		{"seriesByTag('')", 0, "", "", true},
		// incomplete
		{"seriesByTag('key=value)", 0, "", "", true},
		// missing quote
		{"seriesByTag(key=value)", 0, "", "", true},
		// info about _tag "directory"
		{"seriesByTag('key=value')", 0, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 1, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 2, "", "", true},
		// test case for wildcarded name, must be not first check
		{"seriesByTag('name=*', 'key=value')", 0, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 1, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 2, "", "", true},
		{"seriesByTag('name=*', 'key=value*')", 0, "(Tag1 LIKE '__name__=%') AND (arrayExists((x) -> x LIKE 'key=value%', Tags))", "", false},
		{"seriesByTag('name=rps')", 0, "Tag1='__name__=rps'", "", false},
		{"seriesByTag('name=~cpu.usage')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", false},
		{"seriesByTag('name=~cpu.usage')", 1, "", "", true},
		{"seriesByTag('name=~cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", false},
		{"seriesByTag('name=~cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", false},
		{"seriesByTag('name=~^cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", false},
		{"seriesByTag('name=~^cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", false},
		{"seriesByTag('name=rps', 'key=~value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*value'), Tags))", "", false},
		// test for issue #244
		{"seriesByTag('name=rps', 'key=~^value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value.*')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value.*'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^valu[a-e]')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=valu%' AND match(x, '^key=valu[a-e]'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value$')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x='key=value', Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~hello.world')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*hello.world'), Tags))", "", false},
		{`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, 0, `(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'host=%' AND match(x, '^host=.*Vladimirs-MacBook-Pro\\.local'), Tags))`, "", false},
		// grafana multi-value variable produce this
		{"seriesByTag('name=value','what=*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},        // If All masked to value with *
		{"seriesByTag('name=value','what=*x')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false},      // If All masked to value with *
		{"seriesByTag('name=value','what!=*x')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		{"seriesByTag('name={avg,max}')", 0, "Tag1 IN ('__name__=avg','__name__=max')", "", false},
		{"seriesByTag('name=m{in}')", 0, "Tag1='__name__=min'", "", false},
		{"seriesByTag('name=m{in,ax}')", 0, "Tag1 IN ('__name__=min','__name__=max')", "", false},
		{"seriesByTag('name=m{in,ax')", 0, "", "", true},
		{"seriesByTag('name=value','what={avg,max}')", 0, "(Tag1='__name__=value') AND ((has(Tags, 'what=avg')) OR (has(Tags, 'what=max')))", "", false},
		{"seriesByTag('name=value','what!={avg,max}')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		// grafana workaround for multi-value variables default, masked with *
		{"seriesByTag('name=value','what=~*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// empty tag value during autocompletion
		{"seriesByTag('name=value','what=~')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// testcases for useCarbonBehaviour=true
		{"seriesByTag('what=')", 0, "NOT arrayExists((x) -> x LIKE 'what=%', Tags)", "", false},
		{"seriesByTag('name=value','what=')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},
		// testcases for dontMatchMissingTags=false
		{"seriesByTag('key!=value')", 0, "NOT arrayExists((x) -> x='key=value', Tags)", "", false},
		{"seriesByTag('dc!=de', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (NOT arrayExists((x) -> x='dc=de', Tags))", "", false},
		{"seriesByTag('dc!=de', 'cpu!=cpu-total')", 0, "(NOT arrayExists((x) -> x='dc=de', Tags)) AND (NOT arrayExists((x) -> x='cpu=cpu-total', Tags))", "", false},
		{"seriesByTag('dc!=~de|us', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (NOT arrayExists((x) -> x LIKE 'dc=%' AND match(x, '^dc=.*(de|us)'), Tags))", "", false},
	}

	for i, test := range table {
		t.Run(test.query+"#"+strconv.Itoa(i), func(t *testing.T) {
			testName := fmt.Sprintf("query: %#v", test.query)

			config := config.New()
			config.ClickHouse.TagsMinInQuery = test.minTags
			terms, err := ParseSeriesByTag(test.query, config)
			sort.Sort(TaggedTermList(terms))

			if test.isErr {
				if err != nil {
					return
				}
			}
			require.NoError(err, testName+", err")

			var w, pw *where.Where
			if err == nil {
				w, pw, err = TaggedWhere(terms, true, false)
			}

			if test.isErr {
				require.Error(err, testName+", err")
				return
			} else {
				assert.NoError(err, testName+", err")
			}

			assert.Equal(test.where, w.String(), testName+", where")
			assert.Equal(test.prewhere, pw.String(), testName+", prewhere")
		})
	}
}

func TestTaggedWhere_DontMatchMissingTagsFlag(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	table := []struct {
		query    string
		minTags  int
		where    string
		prewhere string
		isErr    bool
	}{
		// test for issue #195
		{"seriesByTag()", 0, "", "", true},
		{"seriesByTag('')", 0, "", "", true},
		// incomplete
		{"seriesByTag('key=value)", 0, "", "", true},
		// missing quote
		{"seriesByTag(key=value)", 0, "", "", true},
		// info about _tag "directory"
		{"seriesByTag('key=value')", 0, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 1, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 2, "", "", true},
		// test case for wildcarded name, must be not first check
		{"seriesByTag('name=*', 'key=value')", 0, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 1, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 2, "", "", true},
		{"seriesByTag('name=*', 'key=value*')", 0, "(Tag1 LIKE '__name__=%') AND (arrayExists((x) -> x LIKE 'key=value%', Tags))", "", false},
		{"seriesByTag('name=rps')", 0, "Tag1='__name__=rps'", "", false},
		{"seriesByTag('name=~cpu.usage')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", false},
		{"seriesByTag('name=~cpu.usage')", 1, "", "", true},
		{"seriesByTag('name=~cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", false},
		{"seriesByTag('name=~cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", false},
		{"seriesByTag('name=~^cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", false},
		{"seriesByTag('name=~^cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", false},
		{"seriesByTag('name=rps', 'key=~value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*value'), Tags))", "", false},
		// test for issue #244
		{"seriesByTag('name=rps', 'key=~^value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value.*')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value.*'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^valu[a-e]')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=valu%' AND match(x, '^key=valu[a-e]'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value$')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x='key=value', Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~hello.world')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*hello.world'), Tags))", "", false},
		{`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, 0, `(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'host=%' AND match(x, '^host=.*Vladimirs-MacBook-Pro\\.local'), Tags))`, "", false},
		// grafana multi-value variable produce this
		{"seriesByTag('name=value','what=*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},   // If All masked to value with *
		{"seriesByTag('name=value','what=*x')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		// {"seriesByTag('name=value','what!=*x')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		{"seriesByTag('name=value','what!=*x')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags) AND NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false},
		{"seriesByTag('name={avg,max}')", 0, "Tag1 IN ('__name__=avg','__name__=max')", "", false},
		{"seriesByTag('name=m{in}')", 0, "Tag1='__name__=min'", "", false},
		{"seriesByTag('name=m{in,ax}')", 0, "Tag1 IN ('__name__=min','__name__=max')", "", false},
		{"seriesByTag('name=m{in,ax')", 0, "", "", true},
		{"seriesByTag('name=value','what={avg,max}')", 0, "(Tag1='__name__=value') AND ((has(Tags, 'what=avg')) OR (has(Tags, 'what=max')))", "", false},
		// {"seriesByTag('name=value','what!={avg,max}')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		{"seriesByTag('name=value','what!={avg,max}')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags) AND NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		// grafana workaround for multi-value variables default, masked with *
		{"seriesByTag('name=value','what=~*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// empty tag value during autocompletion
		{"seriesByTag('name=value','what=~')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// testcases for useCarbonBehaviour=false
		{"seriesByTag('what=')", 0, "Tag1='what='", "", false},
		{"seriesByTag('name=value','what=')", 0, "(Tag1='__name__=value') AND (has(Tags, 'what='))", "", false},
		// testcases for dontMatchMissingTags=true
		{"seriesByTag('key!=value')", 0, "Tag1 LIKE 'key=%' AND NOT arrayExists((x) -> x='key=value', Tags)", "", false},
		{"seriesByTag('dc!=de', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'dc=%', Tags) AND NOT arrayExists((x) -> x='dc=de', Tags))", "", false},
		{"seriesByTag('dc!=de', 'cpu!=cpu-total')", 0, "(Tag1 LIKE 'dc=%' AND NOT arrayExists((x) -> x='dc=de', Tags)) AND (arrayExists((x) -> x LIKE 'cpu=%', Tags) AND NOT arrayExists((x) -> x='cpu=cpu-total', Tags))", "", false},
		{"seriesByTag('dc!=~de|us', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'dc=%', Tags) AND NOT arrayExists((x) -> x LIKE 'dc=%' AND match(x, '^dc=.*(de|us)'), Tags))", "", false},
	}

	for i, test := range table {
		t.Run(test.query+"#"+strconv.Itoa(i), func(t *testing.T) {
			testName := fmt.Sprintf("query: %#v", test.query)

			config := config.New()
			config.ClickHouse.TagsMinInQuery = test.minTags
			terms, err := ParseSeriesByTag(test.query, config)
			sort.Sort(TaggedTermList(terms))

			if test.isErr {
				if err != nil {
					return
				}
			}
			require.NoError(err, testName+", err")

			var w, pw *where.Where
			if err == nil {
				w, pw, err = TaggedWhere(terms, false, true)
			}

			if test.isErr {
				require.Error(err, testName+", err")
				return
			} else {
				assert.NoError(err, testName+", err")
			}

			assert.Equal(test.where, w.String(), testName+", where")
			assert.Equal(test.prewhere, pw.String(), testName+", prewhere")
		})
	}
}

func TestTaggedWhere_BothFeatureFlags(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	table := []struct {
		query    string
		minTags  int
		where    string
		prewhere string
		isErr    bool
	}{
		// test for issue #195
		{"seriesByTag()", 0, "", "", true},
		{"seriesByTag('')", 0, "", "", true},
		// incomplete
		{"seriesByTag('key=value)", 0, "", "", true},
		// missing quote
		{"seriesByTag(key=value)", 0, "", "", true},
		// info about _tag "directory"
		{"seriesByTag('key=value')", 0, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 1, "Tag1='key=value'", "", false},
		{"seriesByTag('key=value')", 2, "", "", true},
		// test case for wildcarded name, must be not first check
		{"seriesByTag('name=*', 'key=value')", 0, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 1, "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value')", 2, "", "", true},
		{"seriesByTag('name=*', 'key=value*')", 0, "(Tag1 LIKE '__name__=%') AND (arrayExists((x) -> x LIKE 'key=value%', Tags))", "", false},
		{"seriesByTag('name=rps')", 0, "Tag1='__name__=rps'", "", false},
		{"seriesByTag('name=~cpu.usage')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", false},
		{"seriesByTag('name=~cpu.usage')", 1, "", "", true},
		{"seriesByTag('name=~cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem)')", false},
		{"seriesByTag('name=~cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*(cpu|mem$)')", false},
		{"seriesByTag('name=~^cpu|mem')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem)')", false},
		{"seriesByTag('name=~^cpu|mem$')", 0, "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=(cpu|mem$)')", false},
		{"seriesByTag('name=rps', 'key=~value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*value'), Tags))", "", false},
		// test for issue #244
		{"seriesByTag('name=rps', 'key=~^value')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value.*')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=value%' AND match(x, '^key=value.*'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^valu[a-e]')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=valu%' AND match(x, '^key=valu[a-e]'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value$')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x='key=value', Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~hello.world')", 0, "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*hello.world'), Tags))", "", false},
		{`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, 0, `(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'host=%' AND match(x, '^host=.*Vladimirs-MacBook-Pro\\.local'), Tags))`, "", false},
		// grafana multi-value variable produce this
		{"seriesByTag('name=value','what=*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},   // If All masked to value with *
		{"seriesByTag('name=value','what=*x')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		// {"seriesByTag('name=value','what!=*x')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		{"seriesByTag('name=value','what!=*x')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags) AND NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false},
		{"seriesByTag('name={avg,max}')", 0, "Tag1 IN ('__name__=avg','__name__=max')", "", false},
		{"seriesByTag('name=m{in}')", 0, "Tag1='__name__=min'", "", false},
		{"seriesByTag('name=m{in,ax}')", 0, "Tag1 IN ('__name__=min','__name__=max')", "", false},
		{"seriesByTag('name=m{in,ax')", 0, "", "", true},
		{"seriesByTag('name=value','what={avg,max}')", 0, "(Tag1='__name__=value') AND ((has(Tags, 'what=avg')) OR (has(Tags, 'what=max')))", "", false},
		// {"seriesByTag('name=value','what!={avg,max}')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		{"seriesByTag('name=value','what!={avg,max}')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags) AND NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		// grafana workaround for multi-value variables default, masked with *
		{"seriesByTag('name=value','what=~*')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// empty tag value during autocompletion
		{"seriesByTag('name=value','what=~')", 0, "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// testcases for useCarbonBehaviour=true
		{"seriesByTag('what=')", 0, "NOT arrayExists((x) -> x LIKE 'what=%', Tags)", "", false},
		{"seriesByTag('name=value','what=')", 0, "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},
		// testcases for dontMatchMissingTags=true
		{"seriesByTag('key!=value')", 0, "Tag1 LIKE 'key=%' AND NOT arrayExists((x) -> x='key=value', Tags)", "", false},
		{"seriesByTag('dc!=de', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'dc=%', Tags) AND NOT arrayExists((x) -> x='dc=de', Tags))", "", false},
		{"seriesByTag('dc!=de', 'cpu!=cpu-total')", 0, "(Tag1 LIKE 'dc=%' AND NOT arrayExists((x) -> x='dc=de', Tags)) AND (arrayExists((x) -> x LIKE 'cpu=%', Tags) AND NOT arrayExists((x) -> x='cpu=cpu-total', Tags))", "", false},
		{"seriesByTag('dc!=~de|us', 'cpu=cpu-total')", 0, "(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'dc=%', Tags) AND NOT arrayExists((x) -> x LIKE 'dc=%' AND match(x, '^dc=.*(de|us)'), Tags))", "", false},
	}

	for i, test := range table {
		t.Run(test.query+"#"+strconv.Itoa(i), func(t *testing.T) {
			testName := fmt.Sprintf("query: %#v", test.query)

			config := config.New()
			config.ClickHouse.TagsMinInQuery = test.minTags
			terms, err := ParseSeriesByTag(test.query, config)
			sort.Sort(TaggedTermList(terms))

			if test.isErr {
				if err != nil {
					return
				}
			}
			require.NoError(err, testName+", err")

			var w, pw *where.Where
			if err == nil {
				w, pw, err = TaggedWhere(terms, true, true)
			}

			if test.isErr {
				require.Error(err, testName+", err")
				return
			} else {
				assert.NoError(err, testName+", err")
			}

			assert.Equal(test.where, w.String(), testName+", where")
			assert.Equal(test.prewhere, pw.String(), testName+", prewhere")
		})
	}
}

func TestParseSeriesByTag(t *testing.T) {
	assert := assert.New(t)

	ok := func(query string, expected []TaggedTerm) {
		config := config.New()
		p, err := ParseSeriesByTag(query, config)
		assert.NoError(err)
		assert.Equal(len(expected), len(p))
		length := len(expected)
		if length < len(p) {
			length = len(p)
		}
		for i := 0; i < length; i++ {
			if i >= len(p) {
				t.Errorf("%s\n- [%d]=%+v", query, i, expected[i])
			} else if i >= len(expected) {
				t.Errorf("%s\n+ [%d]=%+v", query, i, p[i])
			} else if p[i] != expected[i] {
				t.Errorf("%s\n- [%d]=%+v\n+ [%d]=%+v", query, i, expected[i], i, p[i])
			}
		}
	}

	ok(`seriesByTag('key=value')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value"},
	})

	ok(`seriesByTag('name=rps')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "__name__", Value: "rps"},
	})

	ok(`seriesByTag('name=~cpu.usage')`, []TaggedTerm{
		{Op: TaggedTermMatch, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('name!=cpu.usage')`, []TaggedTerm{
		{Op: TaggedTermNe, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('name!=~cpu.usage')`, []TaggedTerm{
		{Op: TaggedTermNotMatch, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "cpu", Value: "cpu-total"},
		{Op: TaggedTermMatch, Key: "host", Value: `Vladimirs-MacBook-Pro\.local`},
	})

}

func newInt(i int) *int {
	p := new(int)
	*p = i
	return p
}

func TestParseSeriesByTagWithCosts(t *testing.T) {
	assert := assert.New(t)

	taggedCosts := map[string]*config.Costs{
		"environment": {Cost: newInt(100)},
		"dc":          {Cost: newInt(60)},
		"project":     {Cost: newInt(50)},
		"__name__":    {Cost: newInt(0), ValuesCost: map[string]int{"high_cost": 70}},
		"key":         {ValuesCost: map[string]int{"value2": 70, "value3": -1, "val*4": -1, "^val.*4$": -1}},
	}

	ok := func(query string, expected []TaggedTerm) {
		config := config.New()
		config.ClickHouse.TaggedCosts = taggedCosts
		terms, err := ParseSeriesByTag(query, config)
		SetCosts(terms, config.ClickHouse.TaggedCosts)
		SortTaggedTermsByCost(terms)
		assert.NoError(err)
		length := len(expected)
		if length < len(terms) {
			length = len(terms)
		}
		for i := 0; i < length; i++ {
			if i >= len(terms) {
				t.Errorf("%s\n- [%d]=%+v", query, i, expected[i])
			} else if i >= len(expected) {
				t.Errorf("%s\n+ [%d]=%+v", query, i, terms[i])
			} else if terms[i] != expected[i] {
				t.Errorf("%s\n- [%d]=%+v\n+ [%d]=%+v", query, i, expected[i], i, terms[i])
			}
		}
	}

	ok(`seriesByTag('environment=production', 'dc=west', 'key=value')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value"},
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
	})

	// Check for values cost (key=value2)
	ok(`seriesByTag('environment=production', 'dc=west', 'key=value2')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "key", Value: "value2", Cost: 70, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
	})

	// Check for __name_ preference
	ok(`seriesByTag('environment=production', 'dc=west', 'key=value', 'name=cpu.load_avg')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 0, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "key", Value: "value"},
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
	})

	// Check for __name_ preference overrided
	ok(`seriesByTag('environment=production', 'dc=west', 'name=cpu.load_avg', 'key=value3')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value3", Cost: -1, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 0, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
	})

	// wildcard (dc=west*)
	ok(`seriesByTag('environment=production', 'dc=west*', 'name=cpu.load_avg', 'key=value3')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value3", Cost: -1, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 0, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "dc", Value: "west*", HasWildcard: true},
	})

	// wildcard cost -1
	ok(`seriesByTag('dc=west*', 'environment=production', 'name=cpu.load_avg', 'key=val*4')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "val*4", Cost: -1, HasWildcard: true, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 0, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "dc", Value: "west*", HasWildcard: true},
	})

	// match cost -1 - not as wildcard
	ok(`seriesByTag('dc=~west.*', 'environment=production', 'name=cpu.load_avg', 'key=~^val.*4$')`, []TaggedTerm{
		{Op: TaggedTermMatch, Key: "key", Value: "^val.*4$", Cost: -1, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 0, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		{Op: TaggedTermMatch, Key: "dc", Value: "west.*"},
	})

	// match cost -1 - and no cost
	ok(`seriesByTag('dc=~west.*', 'environment=production', 'Name=cpu.load_avg', 'key=~^val.*4$')`, []TaggedTerm{
		{Op: TaggedTermMatch, Key: "key", Value: "^val.*4$", Cost: -1, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "Name", Value: "cpu.load_avg"},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		{Op: TaggedTermMatch, Key: "dc", Value: "west.*"},
	})

	// reduce cost for __name__
	ok(`seriesByTag('dc=~west.*', 'environment=production', 'name=high_cost', 'key=~^val.*4$', 'key2=~^val.*4$', 'key3=val.*4')`, []TaggedTerm{
		{Op: TaggedTermMatch, Key: "key", Value: "^val.*4$", Cost: -1, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "__name__", Value: "high_cost", Cost: 70, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		{Op: TaggedTermEq, Key: "key3", Value: "val.*4", HasWildcard: true},
		{Op: TaggedTermMatch, Key: "dc", Value: "west.*"},
		{Op: TaggedTermMatch, Key: "key2", Value: "^val.*4$"},
	})
}

func BenchmarkParseSeriesByTag(b *testing.B) {
	benchmarks := []string{
		"seriesByTag('key=value')",
		"seriesByTag('name=*', 'key=value')",
		"seriesByTag('name=value', '')",
	}
	for _, bm := range benchmarks {
		b.Run(bm, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = ParseSeriesByTag(bm, nil)
			}
		})
	}
}

func TestParseSeriesByTagWithCostsFromCountTable(t *testing.T) {
	assert := assert.New(t)

	var from, until int64
	// 2022-11-11 00:01:00 +05:00 && 2022-11-11 00:01:10 +05:00
	from, until = 1668106860, 1668106870

	taggedCosts := map[string]*config.Costs{
		"environment": {Cost: newInt(100)},
		"dc":          {Cost: newInt(60)},
		"project":     {Cost: newInt(50)},
		"__name__":    {Cost: newInt(0), ValuesCost: map[string]int{"high_cost": 70}},
		"key":         {ValuesCost: map[string]int{"value2": 70, "value3": -1, "val*4": -1, "^val.*4$": -1}},
	}

	ok := func(
		testName, query, sql string,
		response *chtest.TestResponse,
		expected []TaggedTerm,
		metricMightExist bool,
		expectedErr error,
		useTagCostsFromConfig bool,
	) {
		srv := chtest.NewTestServer()
		defer srv.Close()

		cfg, _ := config.DefaultConfig()
		cfg.ClickHouse.URL = srv.URL
		if useTagCostsFromConfig {
			cfg.ClickHouse.TaggedCosts = taggedCosts
		}

		srv.AddResponce(sql, response)

		opts := clickhouse.Options{
			Timeout:        cfg.ClickHouse.IndexTimeout,
			ConnectTimeout: cfg.ClickHouse.ConnectTimeout,
			TLSConfig:      cfg.ClickHouse.TLSConfig,
		}

		taggedFinder := NewTagged(
			cfg.ClickHouse.URL,
			cfg.ClickHouse.TaggedTable,
			"tag1_count_table", // non-empty string, tagged finder will query clickhouse for costs
			true,
			cfg.FeatureFlags.UseCarbonBehavior,
			cfg.FeatureFlags.DontMatchMissingTags,
			false,
			opts,
			cfg.ClickHouse.TaggedCosts,
		)

		stat := &FinderStat{}
		terms, err := taggedFinder.PrepareTaggedTerms(context.Background(), cfg, query, from, until, stat)
		if expectedErr != nil {
			assert.Equal(expectedErr, err, testName+", err")
			return
		}
		assert.NoError(err)
		assert.Equal(metricMightExist, taggedFinder.metricMightExists, testName+", metricMightExist")

		length := len(expected)
		if length < len(terms) {
			length = len(terms)
		}
		for i := 0; i < length; i++ {
			if i >= len(terms) {
				t.Errorf("%s\n- [%d]=%+v", testName, i, expected[i])
			} else if i >= len(expected) {
				t.Errorf("%s\n+ [%d]=%+v", testName, i, terms[i])
			} else if terms[i] != expected[i] {
				t.Errorf("%s\n- [%d]=%+v\n+ [%d]=%+v", testName, i, expected[i], i, terms[i])
			}
		}

	}

	ok(
		`3 TaggedTermEq, database contains all of them`,
		`seriesByTag('environment=production', 'dc=west', 'key=value')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='environment=production') OR (Tag1='dc=west')) OR (Tag1='key=value')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\ndc=west\t10\nkey=value\t1\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "key", Value: "value", Cost: 1, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 10, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		},
		true,
		nil,
		false,
	)

	ok(
		`3 TaggedTermEq, database doesn't contain 1 of them`,
		`seriesByTag('environment=production', 'dc=west', 'key=value')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='environment=production') OR (Tag1='dc=west')) OR (Tag1='key=value')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\nkey=value\t1\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 0, NonDefaultCost: false},
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 0, NonDefaultCost: false},
			{Op: TaggedTermEq, Key: "key", Value: "value", Cost: 0, NonDefaultCost: false},
		},
		false,
		nil,
		false,
	)

	ok(
		`3 TaggedTermEq, database doesn't contain any of them`,
		`seriesByTag('environment=production', 'dc=west', 'key=value')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='environment=production') OR (Tag1='dc=west')) OR (Tag1='key=value')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte(""),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 0, NonDefaultCost: false},
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 0, NonDefaultCost: false},
			{Op: TaggedTermEq, Key: "key", Value: "value", Cost: 0, NonDefaultCost: false},
		},
		false,
		nil,
		false,
	)

	ok(
		`3 TaggedTermEq, one of them has a wildcard`,
		`seriesByTag('environment=production', 'dc=*', 'key=value')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`((Tag1='environment=production') OR (Tag1='key=value')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\nkey=value\t2\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "key", Value: "value", Cost: 2, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "dc", Value: "*", Cost: 0, NonDefaultCost: false, HasWildcard: true},
		},
		true,
		nil,
		false,
	)

	ok(
		`2 TaggedTermEq, 2 TaggedTermMatch`,
		`seriesByTag('environment=production', 'dc=west', 'status=~^o.*', 'key=~val.*')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`((Tag1='environment=production') OR (Tag1='dc=west')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\ndc=west\t10\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 10, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
			{Op: TaggedTermMatch, Key: "status", Value: "^o.*", Cost: 0},
			{Op: TaggedTermMatch, Key: "key", Value: "val.*", Cost: 0},
		},
		true,
		nil,
		false,
	)

	ok(
		`2 TaggedTermEq, 2 TaggedTermNe`,
		`seriesByTag('environment=production', 'dc=west', 'status!=on', 'key!=value')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`((Tag1='environment=production') OR (Tag1='dc=west')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\ndc=west\t10\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 10, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
			{Op: TaggedTermNe, Key: "status", Value: "on", Cost: 0},
			{Op: TaggedTermNe, Key: "key", Value: "value", Cost: 0},
		},
		true,
		nil,
		false,
	)

	ok(
		`2 TaggedTermEq, 2 TaggedTermNotMatch`,
		`seriesByTag('environment=production', 'dc=west', 'status!=~^o.*', 'key!=~val.*')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`((Tag1='environment=production') OR (Tag1='dc=west')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\ndc=west\t10\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 10, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
			{Op: TaggedTermNotMatch, Key: "status", Value: "^o.*", Cost: 0},
			{Op: TaggedTermNotMatch, Key: "key", Value: "val.*", Cost: 0},
		},
		true,
		nil,
		false,
	)

	ok(
		`3 TaggedTermMatch`,
		`seriesByTag('environment=~prod', 'dc=~west', 'key=~^val')`,
		``,
		nil,
		[]TaggedTerm{
			{Op: TaggedTermMatch, Key: "environment", Value: "prod", Cost: 0},
			{Op: TaggedTermMatch, Key: "dc", Value: "west", Cost: 0},
			{Op: TaggedTermMatch, Key: "key", Value: "^val", Cost: 0},
		},
		true,
		nil,
		false,
	)

	ok(
		`3 TaggedTermEq, 2 have same tag`,
		`seriesByTag('environment=production', 'dc=west', 'dc=east')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='environment=production') OR (Tag1='dc=west')) OR (Tag1='dc=east')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\ndc=west\t10\ndc=east\t5\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "dc", Value: "east", Cost: 5, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 10, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		},
		true,
		nil,
		false,
	)

	ok(
		`3 TaggedTermEq, 1 of them has __name__ key`,
		`seriesByTag('name=load.avg', 'environment=production', 'dc=west')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='__name__=load.avg') OR (Tag1='environment=production')) OR (Tag1='dc=west')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\ndc=west\t10\n__name__=load.avg\t10000\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 10, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "__name__", Value: "load.avg", Cost: 10000, NonDefaultCost: true},
		},
		true,
		nil,
		false,
	)

	ok(
		`3 TaggedTermEq, 1 of them has __name__ key, 1 does not exist in count table`,
		`seriesByTag('environment=production', 'dc=west', 'name=load.avg')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='environment=production') OR (Tag1='dc=west')) OR (Tag1='__name__=load.avg')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\n__name__=load.avg\t10000\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "__name__", Value: "load.avg", Cost: 0, NonDefaultCost: false},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 0, NonDefaultCost: false},
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 0, NonDefaultCost: false},
		},
		false,
		nil,
		false,
	)

	ok(
		`Clickhouse returned broken response`,
		`seriesByTag('environment=production', 'dc=west', 'key=value')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='environment=production') OR (Tag1='dc=west')) OR (Tag1='key=value')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("broken_response"),
		},
		nil,
		true,
		fmt.Errorf("failed to parse result from clickhouse while querying for tag costs: no tag count"),
		false,
	)

	ok(
		`3 TaggedTermEq, 1 of them has __name__ key, 1 does not exist in count table, fallback to config costs`,
		`seriesByTag('name=high_cost', 'environment=production', 'dc=west')`,
		`SELECT Tag1, sum(Count) as cnt FROM tag1_count_table WHERE `+
			`(((Tag1='__name__=high_cost') OR (Tag1='environment=production')) OR (Tag1='dc=west')) `+
			`AND (Date >= '`+date.FromTimestampToDaysFormat(from)+`' AND Date <= '`+date.FromTimestampToDaysFormat(until)+`') `+
			`GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		&chtest.TestResponse{
			Body: []byte("environment=production\t100\n__name__=load.avg\t10000\n"),
		},
		[]TaggedTerm{
			{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "__name__", Value: "high_cost", Cost: 70, NonDefaultCost: true},
			{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100, NonDefaultCost: true},
		},
		false,
		nil,
		true,
	)
}

func TestTaggedFinder_whereFilter(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		from         int64
		until        int64
		dailyEnabled bool
		taggedCosts  map[string]*config.Costs
		want         string
		wantPre      string
	}{
		{
			name:         "nodaily",
			query:        "seriesByTag('name=metric')",
			from:         1668106860, // 2022-11-11 00:01:00 +05:00
			until:        1668106870, // 2022-11-11 00:01:10 +05:00
			dailyEnabled: false,
			want: "(Tag1='__name__=metric') AND (Date >='" +
				date.FromTimestampToDaysFormat(1668106860) + "')",
			wantPre: "",
		},
		{
			name:         "midnight at utc (direct)",
			query:        "seriesByTag('name=metric')",
			from:         1668124800, // 2022-11-11 00:00:00 UTC
			until:        1668124810, // 2022-11-11 00:00:10 UTC
			dailyEnabled: true,
			want: "(Tag1='__name__=metric') AND (Date >='" +
				date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668124810) + "')",
			wantPre: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+" "+time.Unix(tt.from, 0).Format(time.RFC3339), func(t *testing.T) {
			config := config.New()
			config.ClickHouse.TaggedCosts = tt.taggedCosts
			terms, err := ParseSeriesByTag(tt.query, config)
			if err != nil {
				t.Fatal(err)
			}
			f := NewTagged(
				"http://localhost:8123/",
				"graphite_tags",
				"",
				tt.dailyEnabled,
				false,
				false,
				false,
				clickhouse.Options{},
				tt.taggedCosts,
			)
			got, gotDate, err := f.whereFilter(terms, tt.from, tt.until)
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tt.want {
				t.Errorf("TaggedFinder.whereFilter()[0] = %v, want %v", got, tt.want)
			}
			if gotDate.String() != tt.wantPre {
				t.Errorf("TaggedFinder.whereFilter()[1] = %v, want %v", gotDate, tt.wantPre)
			}
		})
	}
}

func TestTaggedFinder_Abs(t *testing.T) {
	tests := []struct {
		name   string
		v      []byte
		cached bool
		want   []byte
	}{
		{
			name:   "cached",
			v:      []byte("test_metric;colon=:;forward=/;hash=#;host=127.0.0.1;minus=-;percent=%;plus=+;underscore=_"),
			cached: true,
			want:   []byte("test_metric;colon=:;forward=/;hash=#;host=127.0.0.1;minus=-;percent=%;plus=+;underscore=_"),
		},
		{
			name: "escaped",
			v: []byte(url.QueryEscape("instance:cpu_utilization?ratio_avg") +
				"?" + url.QueryEscape("dc") + "=" + url.QueryEscape("qwe+1") +
				"&" + url.QueryEscape("fqdn") + "=" + url.QueryEscape("asd&a") +
				"&" + url.QueryEscape("instance") + "=" + url.QueryEscape("10.33.10.10:9100") +
				"&" + url.QueryEscape("job") + "=" + url.QueryEscape("node&a")),
			want: []byte("instance:cpu_utilization?ratio_avg;dc=qwe+1;fqdn=asd&a;instance=10.33.10.10:9100;job=node&a"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tf *TaggedFinder
			if tt.cached {
				tf = NewCachedTags(nil)
			} else {
				tf = NewTagged("http:/127.0.0.1:8123", "graphite_tags", "", true, false, false, false, clickhouse.Options{}, nil)
			}
			if got := string(tf.Abs(tt.v)); got != string(tt.want) {
				t.Errorf("TaggedDecode() =\n%q\nwant\n%q", got, string(tt.want))
			}
		})
	}
}
