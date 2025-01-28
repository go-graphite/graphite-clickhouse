package finder

import (
	"fmt"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/stretchr/testify/assert"
)

func Test_splitQuery(t *testing.T) {
	type testcase struct {
		givenQuery      string
		expectedQueries []string
		expectedErr     error
		desc            string
	}

	cases := []testcase{
		{
			givenQuery: "{first,second}.some.metric.*",
			expectedQueries: []string{
				"first.some.metric.*",
				"second.some.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the start of query",
		},
		{
			givenQuery: "some.metric.*.{first,second}",
			expectedQueries: []string{
				"some.metric.*.first",
				"some.metric.*.second",
			},
			expectedErr: nil,
			desc:        "brackets in the end of query",
		},
		{
			givenQuery: "some.{first,second}.metric.*",
			expectedQueries: []string{
				"some.first.metric.*",
				"some.second.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the middle of query",
		},
		{
			givenQuery: "some.{fi*rst,second,th*ird}.metric.*",
			expectedQueries: []string{
				"some.fi*rst.metric.*",
				"some.second.metric.*",
				"some.th*ird.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the middle of query and some have wildcard",
		},
		{
			givenQuery: "some.help_{fi*rst,second,th*ird}_me.metric.*",
			expectedQueries: []string{
				"some.help_fi*rst_me.metric.*",
				"some.help_second_me.metric.*",
				"some.help_th*ird_me.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the middle of query, in the middle of node and some have wildcard",
		},
		{
			givenQuery: "{first,second}.some.{a,b,c}.metric",
			expectedQueries: []string{
				"{first,second}.some.a.metric",
				"{first,second}.some.b.metric",
				"{first,second}.some.c.metric",
			},
			expectedErr: nil,
			desc:        "more than one bracket and reverse preferred",
		},
		{
			givenQuery: "some.{a,b,c}.metric.{first,second}",
			expectedQueries: []string{
				"some.a.metric.{first,second}",
				"some.b.metric.{first,second}",
				"some.c.metric.{first,second}",
			},
			expectedErr: nil,
			desc:        "more than one bracket and direct preferred",
		},
		{
			givenQuery: "some.{a,b}.{first,second}.metric",
			expectedQueries: []string{
				"some.a.{first,second}.metric",
				"some.b.{first,second}.metric",
			},
			expectedErr: nil,
			desc:        "more than one bracket on equal distance, direct preferred",
		},
		{
			givenQuery: "*.{a,b}.{first,second}.metric",
			expectedQueries: []string{
				"*.{a,b}.first.metric",
				"*.{a,b}.second.metric",
			},
			expectedErr: nil,
			desc:        "equal distance, but has wildcard on direct, so reverse preferred",
		},
		{
			givenQuery: "some.{a,b}.{first,second}.metric.hello.*",
			expectedQueries: []string{
				"some.a.{first,second}.metric.hello.*",
				"some.b.{first,second}.metric.hello.*",
			},
			expectedErr: nil,
			desc:        "reverse has more nodes, but also has wildcard, so direct preferred",
		},
		{
			givenQuery: "some.*.{a,b,c}.{first,second}.*.test.metric",
			expectedQueries: []string{
				"some.*.a.{first,second}.*.test.metric",
				"some.*.b.{first,second}.*.test.metric",
				"some.*.c.{first,second}.*.test.metric",
			},
			expectedErr: nil,
			desc:        "have wildcards on both direct and reverse, but leftmost has more choices, so direct preferred",
		},
		{
			givenQuery: "some.*.{a,b}.{first,second,third}.*.test.metric",
			expectedQueries: []string{
				"some.*.{a,b}.first.*.test.metric",
				"some.*.{a,b}.second.*.test.metric",
				"some.*.{a,b}.third.*.test.metric",
			},
			expectedErr: nil,
			desc:        "have wildcards on both direct and reverse, but rightmost has more choices, so reverse preferred",
		},
		{
			givenQuery: "some.{a,b,c}.{first,second}.metric",
			expectedQueries: []string{
				"some.a.{first,second}.metric",
				"some.b.{first,second}.metric",
				"some.c.{first,second}.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, brackets on equal distance, but leftmost has more choices, so direct preferred",
		},
		{
			givenQuery: "some.{a,b}.{first,second,third}.metric",
			expectedQueries: []string{
				"some.{a,b}.first.metric",
				"some.{a,b}.second.metric",
				"some.{a,b}.third.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, brackets on equal distance, but rightmost has more choices, so reverse preferred",
		},
	}

	for i, singleCase := range cases {
		t.Run(fmt.Sprintf("case %v: %s", i+1, singleCase.desc), func(t *testing.T) {
			gotQueries, gotErr := splitQuery(singleCase.givenQuery)

			assert.Equal(t, singleCase.expectedQueries, gotQueries, singleCase.desc)
			assert.Equal(t, singleCase.expectedErr, gotErr, singleCase.desc)
		})
	}
}

func TestSplitIndexFinder_whereFilter(t *testing.T) {
	type testcase struct {
		name                string
		givenQueries        []string
		givenFrom           int64
		givenUntil          int64
		dailyEnabled        bool
		wildcardMinDistance int
		reverse             string
		confReverses        config.IndexReverses
		expectedWhereStr    string
		expectedErr         error
	}

	someFrom := time.Now().Unix() - 120
	someUntil := time.Now().Unix()

	cases := []testcase{
		{
			name: "no wildcards in queries, no daily",
			givenQueries: []string{
				"first.metric",
				"second.metric",
			},
			dailyEnabled:     false,
			expectedWhereStr: "((Path IN ('first.metric','first.metric.','second.metric','second.metric.')) AND (Level=20002)) AND (Date='1970-02-12')",
		},
		{
			name: "wildcard in queries, reverse preferred, no daily",
			givenQueries: []string{
				"*.first.metric",
				"*.second.metric",
			},
			dailyEnabled:     false,
			expectedWhereStr: "(((Path LIKE 'metric.first.%') OR (Path LIKE 'metric.second.%')) AND (Level=30003)) AND (Date='1970-02-12')",
		},
		{
			name: "no wildcards in queries, daily enabled, but no from and until",
			givenQueries: []string{
				"first.metric",
				"second.metric",
			},
			dailyEnabled:     true,
			expectedWhereStr: "((Path IN ('first.metric','first.metric.','second.metric','second.metric.')) AND (Level=20002)) AND (Date='1970-02-12')",
		},
		{
			name: "no wildcards in queries, daily enabled, has from, until",
			givenQueries: []string{
				"first.metric",
				"second.metric",
			},
			givenFrom:    someFrom,
			givenUntil:   someFrom,
			dailyEnabled: true,
			expectedWhereStr: "((Path IN ('first.metric','first.metric.','second.metric','second.metric.')) AND (Level=2)) AND (Date >='" +
				date.FromTimestampToDaysFormat(someFrom) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(someUntil) + "')",
		},
		{
			name: "wildcard in queries, reverse preferred, daily enabled, no from, until",
			givenQueries: []string{
				"*.first.metric",
				"*.second.metric",
			},
			dailyEnabled:     true,
			expectedWhereStr: "(((Path LIKE 'metric.first.%') OR (Path LIKE 'metric.second.%')) AND (Level=30003)) AND (Date='1970-02-12')",
		},
		{
			name: "wildcard in queries, reverse preferred, daily enabled, has from, until",
			givenQueries: []string{
				"*.first.metric",
				"*.second.metric",
			},
			dailyEnabled: true,
			givenFrom:    someFrom,
			givenUntil:   someUntil,
			expectedWhereStr: "(((Path LIKE 'metric.first.%') OR (Path LIKE 'metric.second.%')) AND (Level=10003)) AND (Date >='" +
				date.FromTimestampToDaysFormat(someFrom) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(someUntil) + "')",
		},
		{
			name: "some queries have wildcard, daily enabled, has from, until",
			givenQueries: []string{
				"help.*first.metric",
				"help.second.metric",
				"help.th*rd.metric",
				"help.forth.metric",
			},
			dailyEnabled: true,
			givenFrom:    someFrom,
			givenUntil:   someUntil,
			expectedWhereStr: "((((Path LIKE 'help.%' AND match(Path, '^help[.]([^.]*?)first[.]metric[.]?$')) OR (Path LIKE 'help.th%' AND match(Path, '^help[.]th([^.]*?)rd[.]metric[.]?$'))) OR (Path IN ('help.second.metric','help.second.metric.','help.forth.metric','help.forth.metric.'))) AND (Level=3)) AND (Date >='" +
				date.FromTimestampToDaysFormat(someFrom) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(someUntil) + "')",
		},
		{
			name: "some queries have wildcard, daily enabled, has from, until, but reverse preferred",
			givenQueries: []string{
				"help.*first.metric.count",
				"help.second.metric.count",
				"help.th*rd.metric.count",
				"help.forth.metric.count",
			},
			dailyEnabled: true,
			givenFrom:    someFrom,
			givenUntil:   someUntil,
			expectedWhereStr: "((((Path LIKE 'count.metric.%' AND match(Path, '^count[.]metric[.]([^.]*?)first[.]help[.]?$')) OR (Path LIKE 'count.metric.th%' AND match(Path, '^count[.]metric[.]th([^.]*?)rd[.]help[.]?$'))) OR (Path IN ('count.metric.second.help','count.metric.second.help.','count.metric.forth.help','count.metric.forth.help.'))) AND (Level=10004)) AND (Date >='" +
				date.FromTimestampToDaysFormat(someFrom) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(someUntil) + "')",
			expectedErr: nil,
		},
		{
			name: "some queries have wildcard, daily enabled, has from, until, but reverse preferred, first query has no wildcard",
			givenQueries: []string{
				"help.second.metric.count",
				"help.*first.metric.count",
				"help.th*rd.metric.count",
				"help.forth.metric.count",
			},
			dailyEnabled: true,
			givenFrom:    someFrom,
			givenUntil:   someUntil,
			expectedWhereStr: "((((Path LIKE 'count.metric.%' AND match(Path, '^count[.]metric[.]([^.]*?)first[.]help[.]?$')) OR (Path LIKE 'count.metric.th%' AND match(Path, '^count[.]metric[.]th([^.]*?)rd[.]help[.]?$'))) OR (Path IN ('count.metric.second.help','count.metric.second.help.','count.metric.forth.help','count.metric.forth.help.'))) AND (Level=10004)) AND (Date >='" +
				date.FromTimestampToDaysFormat(someFrom) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(someUntil) + "')",
			expectedErr: nil,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("Case %v: %s", i+1, tc.name), func(t *testing.T) {
			f := WrapSplitIndex(
				&IndexFinder{},
				tc.wildcardMinDistance,
				"http://localhost:8123/",
				"graphite_index",
				tc.dailyEnabled,
				tc.reverse,
				tc.confReverses,
				clickhouse.Options{},
				false)

			got, err := f.whereFilter(tc.givenQueries, tc.givenFrom, tc.givenUntil)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedWhereStr, got.String())
		})
	}
}
