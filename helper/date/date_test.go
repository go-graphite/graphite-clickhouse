package date

import (
	"os"
	"strconv"
	"testing"
	"time"
)

var verbose bool

func isVerbose() bool {
	for _, arg := range os.Args {
		if arg == "-test.v=true" {
			return true
		}
	}
	return false
}

func init() {
	verbose = isVerbose()
}

// TimestampDaysFormat is broken symmetic with carbon-clickhouse of SlowTimestampToDays, not always UTC
// $ TZ=Etc/GMT-5 go test -v -timeout 30s -run ^TestTimestampDaysFormat$ github.com/lomik/graphite-clickhouse/helper/date
// === RUN   TestTimestampDaysFormat
// === RUN   TestTimestampDaysFormat/1668106870_2022-11-11T00:01:10+05:00_2022-11-10T19:01:10Z_[0]
//
//	date_test.go:62: Warning (TimestampDaysFormat broken) TimestampDaysFormat(1668106870) = 2022-11-11, want UTC 2022-11-10
//
// --- FAIL: TestTimestampDaysFormat (0.00s)
//
//	--- FAIL: TestTimestampDaysFormat/1668106870_2022-11-11T00:01:10+05:00_2022-11-10T19:01:10Z_[0] (0.00s)
//
// FAIL
// FAIL	github.com/lomik/graphite-clickhouse/helper/date	0.001s
//
// $ TZ=Etc/GMT+5 go test -v -timeout 30s -run ^TestTimestampDaysFormat$ github.com/lomik/graphite-clickhouse/helper/date
// === RUN   TestTimestampDaysFormat
// === RUN   TestTimestampDaysFormat/1668124800_2022-11-10T19:00:00-05:00_2022-11-11T00:00:00Z_[1]
//
//	date_test.go:62: Warning (TimestampDaysFormat broken) TimestampDaysFormat(1668124800) = 2022-11-10, want UTC 2022-11-11
//
// === RUN   TestTimestampDaysFormat/1668142799_2022-11-10T23:59:59-05:00_2022-11-11T04:59:59Z_[2]
//
//	date_test.go:62: Warning (TimestampDaysFormat broken) TimestampDaysFormat(1668142799) = 2022-11-10, want UTC 2022-11-11
//
// === RUN   TestTimestampDaysFormat/1650776160_2022-04-23T23:56:00-05:00_2022-04-24T04:56:00Z_[3]
//
//	date_test.go:62: Warning (TimestampDaysFormat broken) TimestampDaysFormat(1650776160) = 2022-04-23, want UTC 2022-04-24
//
// --- FAIL: TestTimestampDaysFormat (0.00s)
func TestDefaultTimestampToDaysFormat(t *testing.T) {
	tests := []struct {
		ts   int64
		want string
	}{
		{
			ts: 1668106870, // 2022-11-11 00:01:10 +05:00 ; 2022-11-10 19:01:10 UTC
			// select toDate(1650776160,'UTC')
			//     2022-11-10
			want: time.Unix(1668106870, 0).Format("2006-01-02"),
		},
		{
			ts:   1668124800, // 2022-11-11 00:00:00 UTC
			want: time.Unix(1668124800, 0).Format("2006-01-02"),
		},
		{
			ts:   1668142799, // 2022-11-10 23:59:59 -05:00; 2022-11-11 04:59:59 UTC
			want: time.Unix(1668142799, 0).Format("2006-01-02"),
		},
		{
			ts: 1650776160, // graphite-clickhouse issue #184, graphite-clickhouse in UTC, clickhouse in PDT(UTC-7)
			// 2022-04-24 4:56:00
			// select toDate(1650776160,'UTC')
			//                        2022-04-24
			// select toDate(1650776160,'Etc/GMT+7')
			//                        2022-04-23
			want: time.Unix(1650776160, 0).Format("2006-01-02"),
		},
	}
	for i, tt := range tests {
		t.Run(strconv.FormatInt(tt.ts, 10)+" "+time.Unix(tt.ts, 0).Format(time.RFC3339)+" "+time.Unix(tt.ts, 0).UTC().Format(time.RFC3339)+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			if got := DefaultTimestampToDaysFormat(tt.ts); got != tt.want {
				t.Errorf("DefaultTimestampDaysFormat(%d) = %s, want %s", tt.ts, got, tt.want)
			} else if gotUTC := UTCTimestampToDaysFormat(tt.ts); got != gotUTC {
				// Run to see a warning
				// go test -v -timeout 30s -run ^TestTimestampDaysFormat$ github.com/lomik/graphite-clickhouse/helper/date
				if verbose {
					t.Errorf("Warning (DefaultTimestampDaysFormat broken) DefaultTimestampDaysFormat(%d) = %s, want UTC %s", tt.ts, got, gotUTC)
				} else {
					t.Logf("Warning (DefaultTimestampDaysFormat broken) DefaultTimestampDaysFormat(%d) = %s, want UTC %s", tt.ts, got, gotUTC)
				}
			}
		})
	}
}

func TestDefaultTimeToDaysFormat(t *testing.T) {
	tests := []struct {
		ts   int64
		want string
	}{
		{
			ts: 1668106870, // 2022-11-11 00:01:10 +05:00 ; 2022-11-10 19:01:10 UTC
			// select toDate(1650776160,'UTC')
			//     2022-11-10
			want: time.Unix(1668106870, 0).Format("2006-01-02"),
		},
		{
			ts:   1668124800, // 2022-11-11 00:00:00 UTC
			want: time.Unix(1668124800, 0).Format("2006-01-02"),
		},
		{
			ts:   1668142799, // 2022-11-10 23:59:59 -05:00; 2022-11-11 04:59:59 UTC
			want: time.Unix(1668142799, 0).Format("2006-01-02"),
		},
		{
			ts: 1650776160, // graphite-clickhouse issue #184, graphite-clickhouse in UTC, clickhouse in PDT(UTC-7)
			// 2022-04-24 4:56:00
			// select toDate(1650776160,'UTC')
			//                        2022-04-24
			// select toDate(1650776160,'Etc/GMT+7')
			//                        2022-04-23
			want: time.Unix(1650776160, 0).Format("2006-01-02"),
		},
	}
	for i, tt := range tests {
		t.Run(strconv.FormatInt(tt.ts, 10)+" "+time.Unix(tt.ts, 0).UTC().Format(time.RFC3339)+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			if got := DefaultTimeToDaysFormat(time.Unix(tt.ts, 0)); got != tt.want {
				t.Errorf("DefaultTimeDaysFormat() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestUTCTimestampToDaysFormat(t *testing.T) {
	tests := []struct {
		ts   int64
		want string
	}{
		{
			ts: 1668106870, // 2022-11-11 00:01:10 +05:00 ; 2022-11-10 19:01:10 UTC
			// select toDate(1650776160,'UTC')
			//     2022-11-10
			want: "2022-11-10",
		},
		{
			ts:   1668124800, // 2022-11-11 00:00:00 UTC
			want: "2022-11-11",
		},
		{
			ts:   1668142799, // 2022-11-10 23:59:59 -05:00; 2022-11-11 04:59:59 UTC
			want: "2022-11-11",
		},
		{
			ts: 1650776160, // graphite-clickhouse issue #184, graphite-clickhouse in UTC, clickhouse in PDT(UTC-7)
			// 2022-04-24 4:56:00
			// select toDate(1650776160,'UTC')
			//                        2022-04-24
			// select toDate(1650776160,'Etc/GMT+7')
			//                        2022-04-23
			want: "2022-04-24",
		},
	}
	for i, tt := range tests {
		t.Run(strconv.FormatInt(tt.ts, 10)+" "+time.Unix(tt.ts, 0).Format(time.RFC3339)+" "+time.Unix(tt.ts, 0).UTC().Format(time.RFC3339)+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			if got := UTCTimestampToDaysFormat(tt.ts); got != tt.want {
				t.Errorf("UTCTimestampDaysFormat(%d) = %s, want %s", tt.ts, got, tt.want)
			}
		})
	}
}

func TestUTCTimeToDaysFormat(t *testing.T) {
	tests := []struct {
		ts   int64
		want string
	}{
		{
			ts: 1668106870, // 2022-11-11 00:01:10 +05:00 ; 2022-11-10 19:01:10 UTC
			// select toDate(1650776160,'UTC')
			//     2022-11-10
			want: "2022-11-10",
		},
		{
			ts:   1668124800, // 2022-11-11 00:00:00 UTC
			want: "2022-11-11",
		},
		{
			ts:   1668142799, // 2022-11-10 23:59:59 -05:00; 2022-11-11 04:59:59 UTC
			want: "2022-11-11",
		},
		{
			ts: 1650776160, // graphite-clickhouse issue #184, graphite-clickhouse in UTC, clickhouse in PDT(UTC-7)
			// 2022-04-24 4:56:00
			// select toDate(1650776160,'UTC')
			//                        2022-04-24
			// select toDate(1650776160,'Etc/GMT+7')
			//                        2022-04-23
			want: "2022-04-24",
		},
	}
	for i, tt := range tests {
		t.Run(strconv.FormatInt(tt.ts, 10)+" "+time.Unix(tt.ts, 0).UTC().Format(time.RFC3339)+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			if got := UTCTimeToDaysFormat(time.Unix(tt.ts, 0)); got != tt.want {
				t.Errorf("UTCTimeDaysFormat() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestMinMaxTimestampToDaysFormat(t *testing.T) {
	tests := []struct {
		ts int64
	}{
		{
			ts: 1668106870, // 2022-11-11 00:01:10 +05:00 ; 2022-11-10 19:01:10 UTC
			// select toDate(1650776160,'UTC')
			//     2022-11-10
		},
		{
			ts: 1668124800, // 2022-11-11 00:00:00 UTC
		},
		{
			ts: 1668142799, // 2022-11-10 23:59:59 -05:00; 2022-11-11 04:59:59 UTC
		},
		{
			ts: 1650776160, // graphite-clickhouse issue #184, graphite-clickhouse in UTC, clickhouse in PDT(UTC-7)
			// 2022-04-24 4:56:00
			// select toDate(1650776160,'UTC')
			//                        2022-04-24
			// select toDate(1650776160,'Etc/GMT+7')
			//                        2022-04-23
		},
	}
	for i, tt := range tests {
		t.Run(strconv.FormatInt(tt.ts, 10)+" "+time.Unix(tt.ts, 0).UTC().Format(time.RFC3339)+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			gotMin := MinTimestampToDaysFormat(tt.ts)
			timeMin, _ := time.Parse("2006-01-02", gotMin)
			gotMax := MaxTimestampToDaysFormat(tt.ts)
			timeMax, _ := time.Parse("2006-01-02", gotMax)
			got := DefaultTimestampToDaysFormat(tt.ts)
			tm, _ := time.Parse("2006-01-02", got)

			if timeMin.UnixNano() > timeMax.UnixNano() || tm.UnixNano() > timeMax.UnixNano() || tm.UnixNano() < timeMin.UnixNano() {
				t.Errorf("MinTimeDaysFormat() = %s > MaxTimeDaysFormat() = %s, DefaultTimeDaysFormat() = %s", gotMin, gotMax, got)
			} else {
				t.Logf("MinTimeDaysFormat() = %s, MaxTimeDaysFormat() = %s, DefaultTimeDaysFormat() = %s", gotMin, gotMax, got)
			}
		})
	}
}
