package prometheus

import (
	"fmt"
	"sort"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/prompb"
	"github.com/prometheus/prometheus/pkg/labels"
)

var prompbMatchMap = map[prompb.LabelMatcher_Type]finder.TaggedTermOp{
	prompb.LabelMatcher_EQ:  finder.TaggedTermEq,
	prompb.LabelMatcher_RE:  finder.TaggedTermMatch,
	prompb.LabelMatcher_NEQ: finder.TaggedTermNe,
	prompb.LabelMatcher_NRE: finder.TaggedTermNotMatch,
}

var promqlMatchMap = map[labels.MatchType]finder.TaggedTermOp{
	labels.MatchEqual:     finder.TaggedTermEq,
	labels.MatchNotEqual:  finder.TaggedTermMatch,
	labels.MatchRegexp:    finder.TaggedTermNe,
	labels.MatchNotRegexp: finder.TaggedTermNotMatch,
}

func wherePromPB(matchers []*prompb.LabelMatcher) (string, error) {
	if len(matchers) == 0 {
		return "", nil
	}

	terms := make([]finder.TaggedTerm, 0, len(matchers))
	for i := 0; i < len(matchers); i++ {
		if matchers[i] == nil {
			continue
		}
		op, ok := prompbMatchMap[matchers[i].Type]
		if !ok {
			return "", fmt.Errorf("unknown matcher type %#v", matchers[i].GetType())
		}
		terms = append(terms, finder.TaggedTerm{
			Key:   matchers[i].Name,
			Value: matchers[i].Value,
			Op:    op,
		})
	}

	sort.Sort(finder.TaggedTermList(terms))

	w := finder.NewWhere()
	w.And(finder.TaggedTermWhere1(&terms[0]))

	for i := 1; i < len(terms); i++ {
		w.And(finder.TaggedTermWhereN(&terms[i]))
	}

	return w.String(), nil
}

func wherePromQL(matchers []*labels.Matcher) (string, error) {
	if len(matchers) == 0 {
		return "", nil
	}

	terms := make([]finder.TaggedTerm, 0, len(matchers))
	for i := 0; i < len(matchers); i++ {
		if matchers[i] == nil {
			continue
		}
		op, ok := promqlMatchMap[matchers[i].Type]
		if !ok {
			return "", fmt.Errorf("unknown matcher type %#v", matchers[i].Type)
		}
		terms = append(terms, finder.TaggedTerm{
			Key:   matchers[i].Name,
			Value: matchers[i].Value,
			Op:    op,
		})
	}

	sort.Sort(finder.TaggedTermList(terms))

	w := finder.NewWhere()
	w.And(finder.TaggedTermWhere1(&terms[0]))

	for i := 1; i < len(terms); i++ {
		w.And(finder.TaggedTermWhereN(&terms[i]))
	}

	return w.String(), nil
}
