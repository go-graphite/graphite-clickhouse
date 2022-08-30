package utils

import (
	"fmt"
	"testing"
	"time"
)

func TestTimestampTruncate(t *testing.T) {
	// reverse sorted

	tests := []struct {
		ts       int64
		duration time.Duration
		want     int64
	}{
		{
			ts:       1628876563,
			duration: 2 * time.Second,
			want:     1628876562,
		},
		{
			ts:       1628876563,
			duration: 10 * time.Second,
			want:     1628876560,
		},
		{
			ts:       1628876563,
			duration: time.Minute,
			want:     1628876520,
		},
		{
			ts:       1628876563,
			duration: time.Hour,
			want:     1628874000,
		},
		{
			ts:       1628876563,
			duration: 24 * time.Hour,
			want:     1628812800,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			if got := TimestampTruncate(tt.ts, tt.duration); got != tt.want {
				t.Errorf("timestampTruncate(%d, %d) = %v, want %v", tt.ts, tt.duration, got, tt.want)
			}
		})
	}
}
