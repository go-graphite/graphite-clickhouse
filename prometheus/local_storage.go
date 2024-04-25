//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/web"
)

var _ web.LocalStorage = &storageImpl{}

func (s *storageImpl) CleanTombstones() error {
	return nil
}

func (s *storageImpl) Delete(ctx context.Context, mint, maxt int64, ms ...*labels.Matcher) error {
	return nil
}

func (s *storageImpl) Snapshot(dir string, withHead bool) error {
	return nil
}

func (s *storageImpl) Stats(statsByLabelName string, limit int) (*tsdb.Stats, error) {
	return &tsdb.Stats{
		IndexPostingStats: &index.PostingsStats{},
	}, nil
}

func (s *storageImpl) WALReplayStatus() (tsdb.WALReplayStatus, error) {
	return tsdb.WALReplayStatus{}, nil
}
