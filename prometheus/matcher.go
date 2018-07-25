package prometheus

import (
	"fmt"
	"sort"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/prompb"
)

var OpMap = map[prompb.LabelMatcher_Type]finder.TaggedTermOp{
	prompb.LabelMatcher_EQ:  finder.TaggedTermEq,
	prompb.LabelMatcher_RE:  finder.TaggedTermMatch,
	prompb.LabelMatcher_NEQ: finder.TaggedTermNe,
	prompb.LabelMatcher_NRE: finder.TaggedTermNotMatch,
}

func Where(matchers []*prompb.LabelMatcher) (string, error) {
	if len(matchers) == 0 {
		return "", nil
	}

	terms := make([]finder.TaggedTerm, 0, len(matchers))
	for i := 0; i < len(matchers); i++ {
		if matchers[i] == nil {
			continue
		}
		op, ok := OpMap[matchers[i].Type]
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
