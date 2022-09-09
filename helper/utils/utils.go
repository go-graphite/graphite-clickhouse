package utils

import (
	"strings"
	"time"
)

// TimestampTruncate truncate timestamp with duration
func TimestampTruncate(ts int64, duration time.Duration) int64 {
	tm := time.Unix(ts, 0).UTC()
	return tm.Truncate(duration).UTC().Unix()
}

func Max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func IntersectionPrefix(a, b string) string {
	for i := 0; i < Min(len(a), len(b)); i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a
}

func IntersectionSuffix(a, b string) string {
	na := len(a) - 1
	nb := len(b) - 1
	for i := 0; i < Min(len(a), len(b)); i++ {
		if a[na-i] != b[nb-i] {
			return a[na-i+1:]
		}
	}
	return a
}

func LastNode(s string) string {
	if last := strings.LastIndexByte(s, '.'); last > 0 {
		return s[last+1:]
	}
	return s
}
