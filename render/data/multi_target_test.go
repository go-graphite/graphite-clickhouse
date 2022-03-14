package data

import (
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
)

func Test_getDataTimeout(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.Config
		m    *MultiTarget
		want time.Duration
	}{
		{
			name: "one DataTimeout",
			cfg: &config.Config{
				ClickHouse: config.ClickHouse{
					DataTimeout: time.Second,
					QueryParams: []config.QueryParam{
						{ // default params
							Duration:    0,
							DataTimeout: time.Second,
						},
					},
				},
			},
			m: &MultiTarget{
				TimeFrame{
					From:  1647198000,
					Until: 1647234000,
				}: &Targets{},
			},
			want: time.Second,
		},
		{
			name: "default DataTimeout",
			cfg: &config.Config{
				ClickHouse: config.ClickHouse{
					DataTimeout: time.Second,
					QueryParams: []config.QueryParam{
						{ // default params
							Duration:    0,
							DataTimeout: time.Second,
						},
						{
							Duration:    time.Hour,
							DataTimeout: time.Minute,
						},
					},
				},
			},
			m: &MultiTarget{
				TimeFrame{ // 1 hour - 1s
					From:  1647198000,
					Until: 1647201600 - 1,
				}: &Targets{},
			},
			want: time.Second,
		},
		{
			name: "1m DataTimeout (1 param), select 1h duration",
			cfg: &config.Config{
				ClickHouse: config.ClickHouse{
					DataTimeout: time.Second * 10,
					QueryParams: []config.QueryParam{
						{ // default params
							Duration:    0,
							DataTimeout: time.Second,
						},
						{
							Duration:    time.Hour,
							DataTimeout: time.Minute,
						},
					},
				},
			},
			m: &MultiTarget{
				TimeFrame{ // 1 hour
					From:  1647198000,
					Until: 1647201600,
				}: &Targets{},
			},
			want: time.Minute,
		},
		{
			name: "1m DataTimeout (2 param), select 1h duration",
			cfg: &config.Config{
				ClickHouse: config.ClickHouse{
					DataTimeout: time.Second,
					QueryParams: []config.QueryParam{
						{ // default params
							Duration:    0,
							DataTimeout: time.Second,
						},
						{
							Duration:    time.Hour,
							DataTimeout: time.Minute,
						},
						{
							Duration:    time.Hour * 2,
							DataTimeout: 10 * time.Minute,
						},
					},
				},
			},
			m: &MultiTarget{
				TimeFrame{ // 1 hour
					From:  1647198000,
					Until: 1647201600,
				}: &Targets{},
			},
			want: time.Minute,
		},
		{
			name: "10m DataTimeout (2 param), select 2h1s duration",
			cfg: &config.Config{
				ClickHouse: config.ClickHouse{
					DataTimeout: time.Second,
					QueryParams: []config.QueryParam{
						{ // default params
							Duration:    0,
							DataTimeout: time.Second,
						},
						{
							Duration:    time.Hour,
							DataTimeout: time.Minute,
						},
						{
							Duration:    time.Hour * 2,
							DataTimeout: 10 * time.Minute,
						},
					},
				},
			},
			m: &MultiTarget{
				TimeFrame{ // 2 hour 1s
					From:  1647198000,
					Until: 1647205201,
				}: &Targets{},
			},
			want: 10 * time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDataTimeout(tt.cfg, tt.m); got != tt.want {
				t.Errorf("getDataTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}
