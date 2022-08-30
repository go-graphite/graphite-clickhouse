package utils

import "time"

// TimestampTruncate truncate timestamp with duration
func TimestampTruncate(ts int64, duration time.Duration) int64 {
	tm := time.Unix(ts, 0).UTC()
	return tm.Truncate(duration).UTC().Unix()
}
