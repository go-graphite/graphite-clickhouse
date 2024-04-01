//go:build !noprom
// +build !noprom

package prometheus

import (
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestQuerier_timeRange(t *testing.T) {
	timeNow = func() time.Time {
		// 2022-11-29 09:30:47 UTC
		return time.Unix(1669714247, 0)
	}
	cfg := &config.Config{
		ClickHouse: config.ClickHouse{
			TaggedAutocompleDays: 4,
		},
	}
	tests := []struct {
		name string

		mint  int64
		maxt  int64
		hints *storage.SelectHints

		wantFrom  int64
		wantUntil int64
	}{
		{
			name:      "default from/until",
			wantFrom:  1669368647, // timeNow() - config.Clickhouse.TaggedAutocompleDays
			wantUntil: 1669714247, // timeNow() result
		},
		{
			name: "start/end in SelectHints",
			hints: &storage.SelectHints{
				Start: 1669453200000,
				End:   1669626000000,
			},
			wantFrom:  1669453200,
			wantUntil: 1669626000,
		},
		{
			name: "start/end in SelectHints overflow",
			hints: &storage.SelectHints{
				// ClickHouse supported range of values by the Date type:  [1970-01-01, 2149-06-06]
				Start: 5662310400001,
				End:   5662310400100,
			},
			wantFrom:  1669368647, // timeNow() - config.Clickhouse.TaggedAutocompleDays
			wantUntil: 1669714247, // timeNow() result
		},
		{
			name:      "no start/end in SelectHints",
			hints:     &storage.SelectHints{},
			mint:      1669194000000,
			maxt:      1669280400000,
			wantFrom:  1669194000,
			wantUntil: 1669280400,
		},
		{
			name:  "no start/end in SelectHints, mint/maxt overflow",
			hints: &storage.SelectHints{},
			// ClickHouse supported range of values by the Date type:  [1970-01-01, 2149-06-06]
			mint:      5662310400001,
			maxt:      5662310400100,
			wantFrom:  1669368647, // timeNow() - config.Clickhouse.TaggedAutocompleDays
			wantUntil: 1669714247, // timeNow() result
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStorage(cfg)

			// Querier returns a new Querier on the storage.
			sq, err := s.Querier(tt.mint, tt.maxt)
			require.NoError(t, err)
			q := sq.(*Querier)

			gotFrom, gotUntil := q.timeRange(tt.hints)
			if gotFrom != tt.wantFrom {
				t.Errorf("Querier.timeRange().from got = %v, want %v", gotFrom, tt.wantFrom)
			}
			if gotUntil != tt.wantUntil {
				t.Errorf("Querier.timeRange().until got = %v, want %v", gotUntil, tt.wantUntil)
			}
		})
	}
}
