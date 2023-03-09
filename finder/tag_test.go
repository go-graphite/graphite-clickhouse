package finder

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	chtest "github.com/lomik/graphite-clickhouse/helper/tests/clickhouse"
)

func TestTagsMakeSQL(t *testing.T) {
	assert := assert.New(t)

	tag1Base := "SELECT Tag1 FROM table WHERE "
	tag1Group := " GROUP BY Tag1"

	tagNBase := "SELECT TagN FROM table ARRAY JOIN Tags AS TagN WHERE "
	tagNGroup := " GROUP BY TagN"

	table := []struct {
		query string
		sql   string
		error bool
	}{
		// SELECT Tag1 FROM graphite_tag WHERE Version >= (SELECT Max(Version) FROM graphite_tag WHERE Tag1='' AND Level=0 AND Path='') AND Level=1 GROUP BY Tag1;
		{"_tag", "", false},
		{"_tag.*", tag1Base + "Level=1" + tag1Group, false},
		{"_tag.t1", tag1Base + "(Tag1='t1') AND (Level=1)" + tag1Group, false},
		{"_tag.p1=", tag1Base + "(Tag1 LIKE 'p1=%') AND (Level=1)" + tag1Group, false},
		{"_tag.p1=.*", tag1Base + "(Tag1 LIKE 'p1=%') AND (Level=1)" + tag1Group, false},
		{"_tag.p1=.v1", tag1Base + "(Tag1='p1=v1') AND (Level=1)" + tag1Group, false},
		{"_tag.t2._tag.*", tagNBase + "(Tag1='t2') AND (IsLeaf=1)" + tagNGroup, false},
		{"_tag.t2._tag.t2._tag.p3=.*", tagNBase + "(((Tag1='t2') AND (arrayExists((x) -> x='t2', Tags))) AND (TagN LIKE 'p3=%')) AND (IsLeaf=1)" + tagNGroup, false},
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		m := NewMockFinder([][]byte{[]byte("mock")})
		f := WrapTag(m, "http://localhost:8123/", "table", clickhouse.Options{Timeout: time.Second, ConnectTimeout: time.Second})

		sql, err := f.MakeSQL(test.query)

		if test.error {
			assert.Error(err)
		} else {
			assert.NoError(err)
		}
		assert.Equal(test.sql, sql, testName)
	}
}

func _TestTags(t *testing.T) {
	assert := assert.New(t)

	mockData := [][]byte{[]byte("mock")}

	type w []string
	mock := w{"mock"}
	empty := w{}

	table := []struct {
		query          string
		expectedList   []string
		expectedSeries []string
	}{
		// not tagged query
		{"", mock, mock},
		{"t*", mock, mock},
		{"hello.*", mock, mock},

		// list root
		{"*", w{"_tag.", "mock"}, mock},

		// info about _tag "directory"
		{"_tag", w{"_tag."}, empty},
		{"_tag.*", w{"_tag.t1.", "_tag.t2."}, empty},
		{"_tag.t1", w{"_tag.t1.", "_tag.t2."}, empty},
		{"_tag.t1.*", w{"_tag.t1.", "_tag.t2."}, empty},
		{"_tag.t1._tag.*", w{"_tag.t1.", "_tag.t2."}, empty},
		{"_tag.t1._tag.param=", w{"_tag.t1.", "_tag.t2."}, empty},
		{"_tag.t1._tag.param=.value", w{"_tag.t1.", "_tag.t2."}, empty},
		{"_tag.t1._tag.param=.value.*", w{"_tag.t1.", "_tag.t2."}, empty},

		// {"hello", []string{"hello."}, []string{}},
		// {"hello.*", []string{"hello.world"}, []string{"world"}},
		// {"*.*", []string{"hello.world"}, []string{"world"}},
		// {"*404*", []string{}, []string{}},
		// {"*404*.*", []string{}, []string{}},
		// {"hello.[bad regexp", []string{}, []string{}},
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		srv := chtest.NewTestServer()

		m := NewMockFinder(mockData)
		f := WrapTag(m, srv.URL, "graphite_tag", clickhouse.Options{Timeout: time.Second, ConnectTimeout: time.Second})

		var stat FinderStat
		config := config.New()
		f.Execute(context.Background(), config, test.query, 0, 0, &stat)

		list := make([]string, 0)
		for _, r := range f.List() {
			list = append(list, string(r))
		}

		series := make([]string, 0)
		for _, r := range f.Series() {
			series = append(series, string(r))
		}

		assert.Equal(test.expectedList, list, testName+", list")
		assert.Equal(test.expectedSeries, series, testName+", series")

		srv.Close()
	}
}
