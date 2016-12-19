package finder

import (
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/uber-go/zap"
)

type Finder interface {
	Wrap(Finder) Finder
	WithConfig(*config.Config) Finder
	WithLogger(*zap.Logger) Finder
	Execute(query string) error
	List() [][]byte
	Abs([]byte) []byte
}
