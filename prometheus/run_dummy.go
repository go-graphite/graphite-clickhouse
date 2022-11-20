//go:build noprom
// +build noprom

package prometheus

import (
	"github.com/lomik/graphite-clickhouse/config"
)

func Run(config *config.Config) error {
	return nil
}
