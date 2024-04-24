//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/prometheus/prometheus/storage"
)

type storageImpl struct {
	config *config.Config
}

var _ storage.Storage = &storageImpl{}

func newStorage(config *config.Config) *storageImpl {
	return &storageImpl{config: config}
}

// Querier returns a new Querier on the storage.
func (s *storageImpl) Querier(mint, maxt int64) (storage.Querier, error) {
	return &Querier{
		config: s.config,
		mint:   mint,
		maxt:   maxt,
	}, nil
}

// ChunkQuerier ...
func (s *storageImpl) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return nil, nil
}

// Appender ...
func (s *storageImpl) Appender(ctx context.Context) storage.Appender {
	return nil
}

// StartTime ...
func (s *storageImpl) StartTime() (int64, error) {
	return 0, nil
}

// Close ...
func (s *storageImpl) Close() error {
	return nil
}
