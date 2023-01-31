package render

import (
	"fmt"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
)

func Test_getCacheTimeout(t *testing.T) {
	cacheConfig := config.CacheConfig{
		ShortTimeoutSec:     60,
		ShortTimeoutStr:     "60",
		DefaultTimeoutSec:   300,
		DefaultTimeoutStr:   "300",
		ShortDuration:       3 * time.Hour,
		ShortUntilOffsetSec: 120,
	}

	now := int64(1636985018)

	tests := []struct {
		name    string
		now     time.Time
		from    int64
		until   int64
		want    int32
		wantStr string
	}{
		{
			name:    "short: from = now - 600, until = now - 120",
			now:     time.Unix(now, 0),
			from:    now - 600,
			until:   now - 120,
			want:    60,
			wantStr: "60",
		},
		{
			name:    "short: from = now - 10800",
			now:     time.Unix(now, 0),
			from:    now - 10800,
			until:   now,
			want:    60,
			wantStr: "60",
		},
		{
			name:    "short: from = now - 10810, until = now - 120",
			now:     time.Unix(now, 0),
			from:    now - 10800,
			until:   now - 120,
			want:    60,
			wantStr: "60",
		},
		{
			name:    "short: from = now - 10800, until now - 121",
			now:     time.Unix(now, 0),
			from:    now - 10800,
			until:   now - 121,
			want:    300,
			wantStr: "300",
		},
		{
			name:    "default: from = now - 10801",
			now:     time.Unix(now, 0),
			from:    now - 10801,
			until:   now,
			want:    300,
			wantStr: "300",
		},
		{
			name:    "short: from = now - 122, until = now - 121",
			now:     time.Unix(now, 0),
			from:    now - 122,
			until:   now - 121,
			want:    300,
			wantStr: "300",
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("[%d] %s", i, tt.name), func(t *testing.T) {
			got, gotStr, _ := getCacheTimeout(tt.now, tt.from, tt.until, &cacheConfig)
			if got != tt.want {
				t.Errorf("getCacheTimeout() = %v, want %v", got, tt.want)
			}
			if gotStr != tt.wantStr {
				t.Errorf("getCacheTimeout() = %q, want %q", gotStr, tt.wantStr)
			}
		})
	}
}
