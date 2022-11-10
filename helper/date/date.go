package date

import "time"

var FromTimestampToDaysFormat func(int64) string
var FromTimeToDaysFormat func(time.Time) string
var UntilTimestampToDaysFormat func(int64) string
var UntilTimeToDaysFormat func(time.Time) string

// SetDefault() is for broken SlowTimestampToDays in carbon-clickhouse
func SetDefault() {
	FromTimestampToDaysFormat = DefaultTimestampToDaysFormat
	FromTimeToDaysFormat = DefaultTimeToDaysFormat
	UntilTimestampToDaysFormat = DefaultTimestampToDaysFormat
	UntilTimeToDaysFormat = DefaultTimeToDaysFormat
}

// SetUTC() is for UTCTimestampToDays in carbon-clickhouse (see https://github.com/go-graphite/carbon-clickhouse/pull/114)
func SetUTC() {
	FromTimestampToDaysFormat = UTCTimestampToDaysFormat
	FromTimeToDaysFormat = UTCTimeToDaysFormat
	UntilTimestampToDaysFormat = UTCTimestampToDaysFormat
	UntilTimeToDaysFormat = UTCTimeToDaysFormat
}

// SetBoth() is for mixed  SlowTimestampToDays/UTCTimestampToDays (before rebuild tables complete)
func SetBoth() {
	FromTimestampToDaysFormat = MinTimestampToDaysFormat
	FromTimeToDaysFormat = MinTimeToDaysFormat
	UntilTimestampToDaysFormat = MaxTimestampToDaysFormat
	UntilTimeToDaysFormat = MaxTimeToDaysFormat
}

func init() {
	SetDefault()
}

// from carbon-clickhouse, port of SlowTimestampToDays, broken symmetic, not always UTC
func DefaultTimestampToDaysFormat(ts int64) string {
	t := time.Unix(ts, 0)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02")
}

// from carbon-clickhouse, port of SlowTimestampToDays, broken symmetic, not always UTC
func DefaultTimeToDaysFormat(t time.Time) string {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02")
}

func UTCTimestampToDaysFormat(timestamp int64) string {
	return time.Unix(timestamp, 0).UTC().Format("2006-01-02")
}

func UTCTimeToDaysFormat(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}

func defaultDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func minLocalAndUTC(t time.Time) time.Time {
	tu := defaultDate(t.UTC())
	td := defaultDate(t)
	if tu.Unix() < td.Unix() {
		return tu
	} else {
		return td
	}
}

// MinTimestampToDaysFormat return formatted minimum (local, UTC) date
func MinTimestampToDaysFormat(ts int64) string {
	t := minLocalAndUTC(time.Unix(ts, 0))
	return t.Format("2006-01-02")
}

// MinTimeToDaysFormat return formatted minimum (local, UTC) date
func MinTimeToDaysFormat(t time.Time) string {
	t = minLocalAndUTC(t)
	return t.Format("2006-01-02")
}

func maxLocalAndUTC(t time.Time) time.Time {
	tu := defaultDate(t.UTC())
	td := defaultDate(t)
	if tu.Unix() > td.Unix() {
		return tu
	} else {
		return td
	}
}

// MaxTimestampToDaysFormat return formatted maximum (local, UTC) date
func MaxTimestampToDaysFormat(ts int64) string {
	t := maxLocalAndUTC(time.Unix(ts, 0))
	return t.Format("2006-01-02")
}

// MaxTimeToDaysFormat return formatted maximum (local, UTC) date
func MaxTimeToDaysFormat(t time.Time) string {
	t = maxLocalAndUTC(t)
	return t.Format("2006-01-02")
}
