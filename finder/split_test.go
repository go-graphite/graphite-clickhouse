package finder

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/errs"
	"github.com/stretchr/testify/assert"
)

func Test_splitQuery(t *testing.T) {
	type testcase struct {
		givenQuery               string
		givenMaxNodeToSplitIndex int
		expectedQueries          []string
		expectedErr              error
		desc                     string
	}

	cases := []testcase{
		{
			givenQuery:               "some.*.{a,b,c}.{first,second}.*.test.metric",
			givenMaxNodeToSplitIndex: 3,
			expectedQueries: []string{
				"some.*.{a,b,c}.{first,second}.*.test.metric",
			},
			expectedErr: nil,
			desc:        "have wildcards on both direct and reverse, so no split",
		},
		{
			givenQuery:               "some.long.{a,b,c}.{first,second}.*.metric",
			givenMaxNodeToSplitIndex: 1,
			expectedQueries: []string{
				"some.long.{a,b,c}.{first,second}.*.metric",
			},
			expectedErr: nil,
			desc:        "have wildcard on reverse but index of list is greater than given in config",
		},
		{
			givenQuery:               "some.long.{a,b,c}.{first,second}.*.metric",
			givenMaxNodeToSplitIndex: 2,
			expectedQueries: []string{
				"some.long.a.{first,second}.*.metric",
				"some.long.b.{first,second}.*.metric",
				"some.long.c.{first,second}.*.metric",
			},
			expectedErr: nil,
			desc:        "have wildcard on reverse and index of list is less or equal than given in config",
		},
		{
			givenQuery:               "some.*.{a,b,c}.{first,second}.test.metric",
			givenMaxNodeToSplitIndex: 1,
			expectedQueries: []string{
				"some.*.{a,b,c}.{first,second}.test.metric",
			},
			expectedErr: nil,
			desc:        "have wildcard on direct but index of list is greater than given in config",
		},
		{
			givenQuery:               "some.*.{a,b,c}.{first,second}.test.metric",
			givenMaxNodeToSplitIndex: 2,
			expectedQueries: []string{
				"some.*.{a,b,c}.first.test.metric",
				"some.*.{a,b,c}.second.test.metric",
			},
			expectedErr: nil,
			desc:        "have wildcard on direct and index of list is less or equal than given in config",
		},
		{
			givenQuery:               "some.long.{a,b,c}.{first,second}.test.metric",
			givenMaxNodeToSplitIndex: 1,
			expectedQueries: []string{
				"some.long.{a,b,c}.{first,second}.test.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards on both but indexes of lists are greater than given in config",
		},
		{
			givenQuery:               "{first,second}.some.metric.*",
			givenMaxNodeToSplitIndex: 3,
			expectedQueries: []string{
				"first.some.metric.*",
				"second.some.metric.*",
			},
			expectedErr: nil,
			desc:        "only one bracket with wildcard on reverse",
		},
		{
			givenQuery:               "*.some.metric.{first,second}",
			givenMaxNodeToSplitIndex: 3,
			expectedQueries: []string{
				"*.some.metric.first",
				"*.some.metric.second",
			},
			expectedErr: nil,
			desc:        "only one bracket and wildcard on direct",
		},
		{
			givenQuery:               "some.very.long.{a,b}.*.{first,second}.metric",
			givenMaxNodeToSplitIndex: 2,
			expectedQueries: []string{
				"some.very.long.{a,b}.*.{first,second}.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, but direct has more nodes than max-node-to-split-index",
		},
		{
			givenQuery:               "some.very.long.{a,b}.*.{first,second}.metric",
			givenMaxNodeToSplitIndex: 3,
			expectedQueries: []string{
				"some.very.long.a.*.{first,second}.metric",
				"some.very.long.b.*.{first,second}.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, direct has more nodes than reverse",
		},
		{
			givenQuery:               "some.{a,b}.*.{first,second}.long.test.metric",
			givenMaxNodeToSplitIndex: 2,
			expectedQueries: []string{
				"some.{a,b}.*.{first,second}.long.test.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, but reverse has more nodes than max-node-to-split-index",
		},
		{
			givenQuery:               "some.{a,b}.*.{first,second}.long.test.metric",
			givenMaxNodeToSplitIndex: 3,
			expectedQueries: []string{
				"some.{a,b}.*.first.long.test.metric",
				"some.{a,b}.*.second.long.test.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, reverse has more nodes than direct",
		},
		{
			givenQuery:               "some.very.long.{a,b,c}.*.{first,second}.long.test.metric",
			givenMaxNodeToSplitIndex: 3,
			expectedQueries: []string{
				"some.very.long.a.*.{first,second}.long.test.metric",
				"some.very.long.b.*.{first,second}.long.test.metric",
				"some.very.long.c.*.{first,second}.long.test.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, direct nd reverse has equal nodes, but leftmost has more choices",
		},
		{
			givenQuery:               "some.very.long.{a,b}.*.{first,second,third}.long.test.metric",
			givenMaxNodeToSplitIndex: 3,
			expectedQueries: []string{
				"some.very.long.{a,b}.*.first.long.test.metric",
				"some.very.long.{a,b}.*.second.long.test.metric",
				"some.very.long.{a,b}.*.third.long.test.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, direct nd reverse has equal nodes, but leftmost has more choices",
		},
		{
			givenQuery:               "query.{a,b}",
			givenMaxNodeToSplitIndex: -1,
			expectedQueries: []string{
				"query.{a,b}",
			},
			expectedErr: nil,
			desc:        "not split query",
		},
		{
			givenQuery:               "*.query.{a,b}",
			givenMaxNodeToSplitIndex: -1,
			expectedQueries: []string{
				"*.query.{a,b}",
			},
			expectedErr: nil,
			desc:        "not split query",
		},
	}

	for i, singleCase := range cases {
		t.Run(fmt.Sprintf("case %v: %s", i+1, singleCase.desc), func(t *testing.T) {
			gotQueries, gotErr := splitQuery(singleCase.givenQuery, singleCase.givenMaxNodeToSplitIndex)

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
		},
		{
			name: "queries do not satisfy wildcard min distance",
			givenQueries: []string{
				"a*.first.metric.*",
				"b*.second.metric.*",
			},
			wildcardMinDistance: 1,
			expectedWhereStr:    "",
			expectedErr:         errs.NewErrorWithCode("query has wildcards way too early at the start and at the end of it", http.StatusBadRequest),
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
			if err == nil {
				assert.Equal(t, tc.expectedWhereStr, got.String())
			}
		})
	}
}
