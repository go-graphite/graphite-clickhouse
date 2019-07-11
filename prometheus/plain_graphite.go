package prometheus

import (
	"sort"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
)

type plainGraphiteQuery struct {
	target     string
	nodeLabel  map[int]string
	metricName string
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
