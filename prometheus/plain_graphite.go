package prometheus

import (
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
)

type plainGraphiteQuery struct {
	target     string
	nodeLabel  map[int]string
	metricName string
}

func makePlainGraphiteQuery(matchers ...*labels.Matcher) *plainGraphiteQuery {
	var isMetricNameFound bool
	var target string
	for _, m := range matchers {
		if m.Name == "__name__" && m.Value == "graphite" && m.Type == labels.MatchEqual {
			isMetricNameFound = true
		}
		if m.Name == "target" && m.Type == labels.MatchEqual && m.Value != "" {
			target = m.Value
		}
	}

	// not plain graphite query
	if !isMetricNameFound || target == "" {
		return nil
	}

	q := &plainGraphiteQuery{target: target}
	// fill additional params

	for _, m := range matchers {
		if m.Name == "metric" && m.Type == labels.MatchEqual && m.Value != "" {
			q.metricName = m.Value
		}

		if strings.HasPrefix(m.Name, "node") && m.Type == labels.MatchEqual && m.Value != "" {
			v, err := strconv.Atoi(m.Name[4:])
			if err != nil {
				continue
			}

			if q.nodeLabel == nil {
				q.nodeLabel = make(map[int]string)
			}

			q.nodeLabel[v] = m.Value
		}
	}

	return q
}
