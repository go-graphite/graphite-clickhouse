package finder

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func TestTaggedWhere(t *testing.T) {
	assert := assert.New(t)

	// until := time.Now().Unix()
	// from := until - 5*24*60*60

	table := []struct {
		query    string
		where    string
		prewhere string
		isErr    bool
	}{
		// info about _tag "directory"
		{"seriesByTag('key=value')", "Tag1='key=value'", "", false},
		{"seriesByTag('name=rps')", "Tag1='__name__=rps'", "", false},
		{"seriesByTag('name=~cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=cpu%' AND match(Tag1, '__name__=cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=cpu%' AND match(Tag1, '__name__=cpu.usage')", false},
		{"seriesByTag('name=rps', 'key=~value')", "(Tag1='__name__=rps') AND (arrayExists((x) -> x='key=value', Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~hello.world')", "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=hello%' AND match(x, 'key=hello.world'), Tags))", "", false},
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		srv := clickhouse.NewTestServer()

		f := NewTagged(srv.URL, "tbl", clickhouse.Options{Timeout: time.Second, ConnectTimeout: time.Second})

		w, pw, err := f.makeWhere(test.query)

		assert.Equal(test.where, w, testName+", where")
		assert.Equal(test.prewhere, pw, testName+", prewhere")
		assert.Equal(test.isErr, err != nil, testName+", where")

		srv.Close()
	}
}
