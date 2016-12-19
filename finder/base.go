package finder

import (
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type BaseFinder struct {
	config *config.Config
	logger *zap.Logger
}

func (b *BaseFinder) Wrap(Finder) Finder { return b }

func (b *BaseFinder) WithConfig(cfg *config.Config) Finder {
	b.config = cfg
	return b
}

func (b *BaseFinder) WithLogger(logger *zap.Logger) Finder {
	b.logger = logger
	return b
}
