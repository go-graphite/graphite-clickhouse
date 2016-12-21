package finder

import (
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type BaseFinder struct {
	config *config.Config
	logger *zap.Logger
}

func NewBase(config *config.Config, logger *zap.Logger) Finder {
	return &BaseFinder{
		config: config,
		logger: logger,
	}
}

func (b *BaseFinder) Execute(query string) error {
	return nil
}

func (b *BaseFinder) List() [][]byte {
	return nil
}

func (b *BaseFinder) Series() [][]byte {
	return nil
}

func (b *BaseFinder) Abs([]byte) []byte {
	return nil
}
