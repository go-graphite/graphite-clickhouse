package finder

import (
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type PrefixFinder struct {
	wrapped Finder
	config  *config.Config
	logger  *zap.Logger
	prefix  string
}

func (p *PrefixFinder) Wrap(f Finder) Finder {
	p.wrapped = f
	return p
}

func (p *PrefixFinder) WithConfig(cfg *config.Config) Finder {
	p.config = cfg
	return p
}

func (p *PrefixFinder) WithLogger(logger *zap.Logger) Finder {
	p.logger = logger
	return p
}
