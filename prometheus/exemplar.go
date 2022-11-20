//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type nopExemplarQueryable struct {
}

type nopExemplarQuerier struct {
}

var _ storage.ExemplarQueryable = &nopExemplarQueryable{}
var _ storage.ExemplarQuerier = &nopExemplarQuerier{}

func (e *nopExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &nopExemplarQuerier{}, nil
}

func (e *nopExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	return []exemplar.QueryResult{}, nil
}
