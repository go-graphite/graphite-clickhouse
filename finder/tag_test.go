package finder

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func TestTagsMakeSQL(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query string
		sql   string
		error bool
	}{
		{"_tag", "", false},
		{"_tag.*", "SELECT Tag1 FROM table WHERE (Level=1) GROUP BY Tag1", false},
		{"_tag.t1", "SELECT Tag1 FROM table WHERE (Level=1) AND (Tag1='t1') GROUP BY Tag1", false},
		{"_tag.p1=", "SELECT Tag1 FROM table WHERE (Level=1) AND (Tag1 LIKE 'p1=%') GROUP BY Tag1", false},
		{"_tag.p1=.*", "SELECT Tag1 FROM table WHERE (Level=1) AND (Tag1 LIKE 'p1=%') GROUP BY Tag1", false},
		{"_tag.p1=.v1", "SELECT Tag1 FROM table WHERE (Level=1) AND (Tag1='p1=v1') GROUP BY Tag1", false},
		{"_tag.t2._tag.*", "", false},
		{"_tag.t2._tag.t2._tag.p3=.*", "", false},
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		m := NewMockFinder([][]byte{[]byte("mock")})
		f := WrapTag(m, context.Background(), "http://localhost:8123/", "table", time.Second)

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

		srv := clickhouse.NewTestServer()

		m := NewMockFinder(mockData)
		f := WrapTag(m, context.Background(), srv.URL, "graphite_tag", time.Second)

		f.Execute(test.query)

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
