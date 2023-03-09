package finder

import (
	"context"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
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

func (f *plainFromTaggedFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) error {
	return f.wrappedPlain.Execute(ctx, config, query, from, until, stat)
}

// For Render
func (f *plainFromTaggedFinder) Series() [][]byte {
	return f.wrappedPlain.Series()
}

type taggedLabel struct {
	name  string
	value string
}

func (f *plainFromTaggedFinder) Abs(value []byte) []byte {
	name := "graphite"
	path := string(value)
	lb := []taggedLabel{
		{"metric", path},
	}
	if f.metricName != "" {
		name = f.metricName
	}

	if len(f.nodeLabel) > 0 {
		a := strings.Split(path, ".")
		for n, v := range a {
			l := f.nodeLabel[n]
			if l != "" {
				lb = append(lb, taggedLabel{l, v})
			}
		}
	}

	sort.Slice(lb, func(i, j int) bool { return lb[i].name < lb[j].name })

	var buf strings.Builder

	buf.WriteString(name)
	buf.WriteByte('?')
	for i, l := range lb {
		if i > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(url.QueryEscape(l.name))
		buf.WriteByte('=')
		buf.WriteString(url.QueryEscape(l.value))
	}

	return []byte(buf.String())
}

func (f *plainFromTaggedFinder) List() [][]byte {
	return f.wrappedPlain.List()
}

func (f *plainFromTaggedFinder) Bytes() ([]byte, error) {
	return nil, ErrNotImplemented
}
