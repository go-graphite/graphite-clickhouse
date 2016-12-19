package finder

import (
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type BaseFinder struct {
	config *config.Config
	logger *zap.Logger
}
