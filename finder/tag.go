package finder

import (
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type TagFinder struct {
	wrapped Finder
	config  *config.Config
	logger  *zap.Logger
}

func WrapTag(f Finder, config *config.Config, logger *zap.Logger) Finder {
	return &TagFinder{
		wrapped: f,
		logger:  logger,
		config:  config,
	}
}

func (t *TagFinder) Execute(query string) error {
	return nil
}

func (t *TagFinder) List() [][]byte {
	return nil
}

func (t *TagFinder) Abs([]byte) []byte {
	return nil
}
