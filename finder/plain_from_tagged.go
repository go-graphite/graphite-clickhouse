package finder

import (
	"context"
	"strconv"
	"strings"
)

// Special finder for query plain graphite from prometheus
// graphite{target="telegraf.*.cpu.avg"}
type plainFromTaggedFinder struct {
	wrappedPlain Finder
	target       string
	nodeLabel    map[int]string
	metricName   string
}

func makePlainFromTagged(matchers []TaggedTerm) *plainFromTaggedFinder {
	var isMetricNameFound bool
	var target string
	for _, m := range matchers {
		if m.Key == "__name__" && m.Value == "graphite" && m.Op == TaggedTermEq {
			isMetricNameFound = true
		}
		if m.Key == "target" && m.Op == TaggedTermEq && m.Value != "" {
			target = m.Value
		}
	}

	// not plain graphite query
	if !isMetricNameFound || target == "" {
		return nil
	}

	q := &plainFromTaggedFinder{target: target}
	// fill additional params
	for _, m := range matchers {
		if m.Key == "rename" && m.Op == TaggedTermEq && m.Value != "" {
			q.metricName = m.Value
		}

		if strings.HasPrefix(m.Key, "node") && m.Op == TaggedTermEq && m.Value != "" {
			v, err := strconv.Atoi(m.Key[4:])
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

func (f *plainFromTaggedFinder) Target() string {
	return f.target
}

func (f *plainFromTaggedFinder) Execute(ctx context.Context, query string, from int64, until int64) error {
	return f.wrappedPlain.Execute(ctx, query, from, until)
}

// For Render
func (f *plainFromTaggedFinder) Series() [][]byte {
	return f.wrappedPlain.Series()
}

func (f *plainFromTaggedFinder) Abs(value []byte) []byte {
	// @TODO
	return value
}

func (f *plainFromTaggedFinder) List() [][]byte {
	return f.wrappedPlain.List()
}
