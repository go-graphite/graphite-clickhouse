package data

import (
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
)

type Targets struct {
	// List contains list of metrics in one the target
	List              []string
	AM                *alias.Map
	pointsTable       string
	isReverse         bool
	rollupObj         *rollup.Rules
	rollupUseReverted bool
}

func (tt *Targets) selectDataTable(cfg *config.Config, from int64, until int64, context string) error {
	now := time.Now().Unix()

TableLoop:
	for i := 0; i < len(cfg.DataTable); i++ {
		t := &cfg.DataTable[i]

		if !t.ContextMap[context] {
			continue TableLoop
		}

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
			for j := 0; j < len(tt.List); j++ {
				if !t.TargetMatchAllRegexp.MatchString(tt.List[j]) {
					continue TableLoop
				}
			}
		}

		if t.TargetMatchAnyRegexp != nil {
			matched := false
		TargetsLoop:
			for j := 0; j < len(tt.List); j++ {
				if t.TargetMatchAnyRegexp.MatchString(tt.List[j]) {
					matched = true
					break TargetsLoop
				}
			}
			if !matched {
				continue TableLoop
			}
		}
		tt.pointsTable = t.Table
		tt.isReverse = t.Reverse
		tt.rollupUseReverted = t.RollupUseReverted
		tt.rollupObj = t.Rollup.Rules()
		return nil
	}

	return fmt.Errorf("data tables is not specified for %v", tt.List[0])
}
