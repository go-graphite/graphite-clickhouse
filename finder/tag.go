package finder

import (
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type TagFinder struct {
	wrapped Finder
	config  *config.Config
	logger  *zap.Logger
	prefix  string
}

func (t *TagFinder) Wrap(f Finder) Finder {
	t.wrapped = f
	return t
}

func (t *TagFinder) WithConfig(cfg *config.Config) Finder {
	t.config = cfg
	return t
}

func (t *TagFinder) WithLogger(logger *zap.Logger) Finder {
	t.logger = logger
	return t
}
