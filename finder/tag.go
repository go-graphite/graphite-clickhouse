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
