package render

import (
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
)

func SelectDataTable(cfg *config.Config, from int64, until int64, targets []string) (string, bool, *rollup.Rules) {
	now := time.Now().Unix()

TableLoop:
	for i := 0; i < len(cfg.DataTable); i++ {
		t := &cfg.DataTable[i]

		if t.MaxInterval != nil && (until-from) > int64(t.MaxInterval.Value().Seconds()) {
			continue TableLoop
		}

		if t.MinInterval != nil && (until-from) < int64(t.MinInterval.Value().Seconds()) {
			continue TableLoop
		}

		if t.MaxAge != nil && from < now-int64(t.MaxAge.Value().Seconds()) {
			continue TableLoop

		}

		if t.MinAge != nil && until > now-int64(t.MinAge.Value().Seconds()) {
			continue TableLoop

		}

		if t.TargetMatchAllRegexp != nil {
			for j := 0; j < len(targets); j++ {
				if !t.TargetMatchAllRegexp.MatchString(targets[j]) {
					continue TableLoop
				}
			}
		}

		if t.TargetMatchAnyRegexp != nil {
			matched := false
		TargetsLoop:
			for j := 0; j < len(targets); j++ {
				if t.TargetMatchAnyRegexp.MatchString(targets[j]) {
					matched = true
					break TargetsLoop
				}
			}
			if !matched {
				continue TableLoop
			}
		}

		if t.Rollup != nil {
			return t.Table, t.Reverse, t.Rollup.Rules()
		} else {
			return t.Table, t.Reverse, cfg.ClickHouse.Rollup.Rules()
		}
	}

	return cfg.ClickHouse.DataTable, false, cfg.ClickHouse.Rollup.Rules()
}
