package data

import (
	"fmt"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/stretchr/testify/assert"
)

func TestSelectDataTableTime(t *testing.T) {
	cfg := config.New()
	cfg.DataTable = []config.DataTable{
		{
			Table:  "first_day",
			MaxAge: 24 * time.Hour,
		},
		{
			Table:  "second_day",
			MinAge: 24 * time.Hour,
			MaxAge: 48 * time.Hour,
		},
		{
			Table:       "two_days_min_interval",
			MaxAge:      48 * time.Hour,
			MinInterval: 2 * time.Hour,
		},
		{
			Table:       "two_days_min_max_interval",
			MaxAge:      48 * time.Hour,
			MinInterval: 30 * time.Minute,
			MaxInterval: 1 * time.Hour,
		},
		{
			Table:       "two_days_max_interval",
			MaxAge:      48 * time.Hour,
			MaxInterval: 2 * time.Hour,
		},
		{
			Table:  "three_days",
			MaxAge: 72 * time.Hour,
		},
		{
			Table: "unlimited",
		},
	}
	err := cfg.ProcessDataTables()
	assert.NoError(t, err)
	tg := NewTargets([]string{"metric"}, nil)

	tests := []struct {
		*TimeFrame
		config.DataTable
		err error
	}{
		{
			&TimeFrame{ageToTimestamp(3600*24 - 1), ageToTimestamp(1800), 1},
			cfg.DataTable[0],
			nil,
		},
		{
			&TimeFrame{ageToTimestamp(3600*48 - 1), ageToTimestamp(24*3600 + 1), 1},
			cfg.DataTable[1],
			nil,
		},
		{
			&TimeFrame{ageToTimestamp(3600 * 26), ageToTimestamp(3600 * 23), 1},
			cfg.DataTable[2],
			nil,
		},
		{
			&TimeFrame{ageToTimestamp(3600*24 + 1600), ageToTimestamp(3600*24 - 1600), 1},
			cfg.DataTable[3],
			nil,
		},
		{
			&TimeFrame{ageToTimestamp(3600*24 + 2000), ageToTimestamp(3600*24 - 2000), 1},
			cfg.DataTable[4],
			nil,
		},
		{
			&TimeFrame{ageToTimestamp(3600*72 - 1), ageToTimestamp(3600*11 - 1), 1},
			cfg.DataTable[5],
			nil,
		},
		{
			&TimeFrame{ageToTimestamp(3600 * 100), ageToTimestamp(3600*11 - 1), 1},
			cfg.DataTable[6],
			nil,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%s", i+1, test.DataTable.Table), func(t *testing.T) {
			err := tg.selectDataTable(cfg, test.TimeFrame, config.ContextGraphite)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.DataTable.Table, tg.pointsTable)
		})
	}
}

func TestSelectDataTableMatch(t *testing.T) {
	cfg := config.New()
	cfg.DataTable = []config.DataTable{
		{
			Table:          "all",
			TargetMatchAll: "^all.*avg",
		},
		{
			Table:          "any",
			TargetMatchAny: "^any.*avg",
		},
		{
			Table: "unlimited",
		},
	}
	err := cfg.ProcessDataTables()
	assert.NoError(t, err)
	tf := &TimeFrame{ageToTimestamp(3600*24 - 1), ageToTimestamp(1800), 1}

	tests := []struct {
		*Targets
		config.DataTable
		err error
	}{
		{
			NewTargets([]string{"allinclucive.in.avg", "all.metrics.for.avg"}, nil),
			cfg.DataTable[0],
			nil,
		},
		{
			NewTargets([]string{"allinclucive.in.avg", "any.metrics.for.avg"}, nil),
			cfg.DataTable[1],
			nil,
		},
		{
			NewTargets([]string{"allinclucive.in.avg", "some.metrics.for.avg"}, nil),
			cfg.DataTable[2],
			nil,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%s", i+1, test.DataTable.Table), func(t *testing.T) {
			err := test.Targets.selectDataTable(cfg, tf, config.ContextGraphite)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.DataTable.Table, test.Targets.pointsTable)
		})
	}
}
