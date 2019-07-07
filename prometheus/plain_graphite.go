package prometheus

import (
	"sort"
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

func (q *plainGraphiteQuery) Target() string {
	return q.target
}

func (q *plainGraphiteQuery) Labels(path string) labels.Labels {
	lb := make(labels.Labels, 2)
	lb[0].Name = "__name__"
	if q.metricName != "" {
		lb[0].Value = q.metricName
	} else {
		lb[0].Value = "graphite"
	}

	lb[1].Name = "metric"
	lb[1].Value = path

	if len(q.nodeLabel) > 0 {
		a := strings.Split(path, ".")
		for n, v := range a {
			l := q.nodeLabel[n]
			if l != "" {
				lb = append(lb, labels.Label{Name: l, Value: v})
			}
		}
	}

	if len(lb) > 2 {
		sort.Slice(lb[1:], func(i, j int) bool { return lb[i+1].Name < lb[j+1].Name })
	}

	return lb
}
