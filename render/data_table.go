package render

import (
	"time"

	"github.com/lomik/graphite-clickhouse/config"
)

func SelectDataTable(cfg *config.Config, from int64, until int64) (string, bool) {
	now := time.Now().Unix()

	for i := 0; i < len(cfg.DataTable); i++ {
		t := &cfg.DataTable[i]

		if t.MaxInterval != nil && (until-from) > int64(t.MaxInterval.Value().Seconds()) {
			continue
		}

		if t.MinInterval != nil && (until-from) < int64(t.MinInterval.Value().Seconds()) {
			continue
		}

		if t.MaxAge != nil && from < now-int64(t.MaxAge.Value().Seconds()) {
			continue

		}

		if t.MinAge != nil && until > now-int64(t.MinAge.Value().Seconds()) {
			continue

		}

		return t.Table, t.Reverse
	}

	return cfg.ClickHouse.DataTable, false
}
