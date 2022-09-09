package data

import (
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
)

// Targets represents requested metrics
type Targets struct {
	// List contains queried metrics, e.g. [metric.{name1,name2}, metric.name[3-9]]
	List []string
	// AM stores found expanded metrics
	AM                *alias.Map
	pointsTable       string
	isReverse         bool
	rollupRules       *rollup.Rules
	rollupUseReverted bool
	queryMetrics      *metrics.QueryMetrics
}

func (tt *Targets) selectDataTable(cfg *config.Config, tf *TimeFrame, context string, am *alias.Map) error {
	now := time.Now().Unix()

TableLoop:
	for i := 0; i < len(cfg.DataTable); i++ {
		t := &cfg.DataTable[i]

		if !t.ContextMap[context] {
			continue TableLoop
		}

		if t.MaxInterval != 0 && (tf.Until-tf.From) > int64(t.MaxInterval.Seconds()) {
			continue TableLoop
		}

		if t.MinInterval != 0 && (tf.Until-tf.From) < int64(t.MinInterval.Seconds()) {
			continue TableLoop
		}

		if t.MaxAge != 0 && tf.From < now-int64(t.MaxAge.Seconds()) {
			continue TableLoop

		}

		if t.MinAge != 0 && tf.Until > now-int64(t.MinAge.Seconds()) {
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
		if t.AutoDetect {
			tt.isReverse = am.IsReversePrefered(t.Reverse, t.AutoMinMetrics, t.AutoRevDensity, t.AutoSamples)
			if tt.isReverse {
				tt.pointsTable = t.ReverseTable
			} else {
				tt.pointsTable = t.DirectTable
			}
		} else {
			tt.pointsTable = t.Table
			tt.isReverse = t.Reverse
		}
		tt.rollupUseReverted = t.RollupUseReverted
		tt.rollupRules = t.Rollup.Rules()
		tt.queryMetrics = t.QueryMetrics
		return nil
	}

	return fmt.Errorf("data tables is not specified for %v", tt.List[0])
}
