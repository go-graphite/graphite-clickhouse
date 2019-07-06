package prometheus

import (
	"net/url"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
)

type Labeler interface {
	Labels(path string) labels.Labels
}

type TaggedLabeler struct {
}

var DefaultLabeler Labeler = &TaggedLabeler{}

func urlParse(rawurl string) (*url.URL, error) {
	p := strings.IndexByte(rawurl, '?')
	if p < 0 {
		return url.Parse(rawurl)
	}
	m, err := url.Parse(rawurl[p:])
	if m != nil {
		m.Path = rawurl[:p]
	}
	return m, err
}

func (*TaggedLabeler) Labels(path string) labels.Labels {
	u, err := urlParse(path)
	if err != nil {
		return labels.Labels{labels.Label{Name: "__name__", Value: path}}
	}

	q := u.Query()
	lb := make(labels.Labels, len(q)+1)
	lb[0].Name = "__name__"
	lb[0].Value = u.Path

	i := 0
	for k, v := range q {
		i++
		lb[i].Name = k
		lb[i].Value = v[0]
	}

	if len(lb) > 2 {
		sort.Slice(lb[1:], func(i, j int) bool { return lb[i+1].Name < lb[j+1].Name })
	}

	return lb
}
