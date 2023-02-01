package data

import (
	"fmt"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
)

type Cache struct {
	Cached     bool
	TS         int64 // cached timestamp
	Timeout    int32
	TimeoutStr string
	Key        string // cache key
	M          *metrics.CacheMetric
}

// Targets represents requested metrics
type Targets struct {
	// List contains queried metrics, e.g. [metric.{name1,name2}, metric.name[3-9]]
	List   []string
	Cache  []Cache
	Cached bool // all is cached
	// AM stores found expanded metrics
	AM                *alias.Map
	pointsTable       string
	isReverse         bool
	rollupRules       *rollup.Rules
	rollupUseReverted bool
	queryMetrics      *metrics.QueryMetrics
}

func NewTargets(list []string, am *alias.Map) *Targets {
	return &Targets{
		List:  list,
		Cache: make([]Cache, len(list)),
		AM:    am,
	}
}

func NewTargetsOne(target string, capacity int, am *alias.Map) *Targets {
	list := make([]string, 1, capacity)
	list[0] = target
	return &Targets{
		List:  list,
		Cache: make([]Cache, 1, capacity),
		AM:    am,
	}
}

func (tt *Targets) Append(target string) {
	tt.List = append(tt.List, target)
	tt.Cache = append(tt.Cache, Cache{})
}

func (tt *Targets) selectDataTable(cfg *config.Config, tf *TimeFrame, context string) error {
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
		tt.pointsTable = t.Table
		tt.isReverse = t.Reverse
		tt.rollupUseReverted = t.RollupUseReverted
		tt.rollupRules = t.Rollup.Rules()
		tt.queryMetrics = t.QueryMetrics
		return nil
	}

	return fmt.Errorf("data tables is not specified for %v", tt.List[0])
}
