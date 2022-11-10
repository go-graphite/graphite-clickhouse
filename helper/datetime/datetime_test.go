package datetime

import (
	"testing"
	"time"
)

func TestDateParamToEpoch(t *testing.T) {
	timeZone := time.Local
	//16 Aug 1994 15:30
	now := time.Date(1994, time.August, 16, 15, 30, 0, 100, timeZone)

	const shortForm = "15:04:05 2006-Jan-02"

	var tests = []struct {
		input  string
		output string
	}{
		{"midnight", "00:00:00 1994-Aug-16"},
		{"noon", "12:00:00 1994-Aug-16"},
		{"teatime", "16:00:00 1994-Aug-16"},
		{"tomorrow", "00:00:00 1994-Aug-17"},

		{"noon 08/12/94", "12:00:00 1994-Aug-12"},
		{"midnight 20060812", "00:00:00 2006-Aug-12"},
		{"noon tomorrow", "12:00:00 1994-Aug-17"},

		{"17:04 19940812", "17:04:00 1994-Aug-12"},
		{"-1day", "15:30:00 1994-Aug-15"},
		{"19940812", "00:00:00 1994-Aug-12"},

		{"midnight-10", "23:59:50 1994-Aug-15"},
		{"midnight-1s", "23:59:59 1994-Aug-15"},
		{"midnight-1day", "00:00:00 1994-Aug-15"},
	}

	for _, tt := range tests {
		var (
			want     int64
			wantTime string
		)
		if tt.output != "" {
			ts, err := time.ParseInLocation(shortForm, tt.output, timeZone)
			if err != nil {
				t.Fatalf("error parsing time: %q: %v", tt.output, err)
			}
			want = int64(ts.Unix())
			wantTime = ts.Format(time.RFC3339Nano)
		}

		got := DateParamToEpoch(tt.input, timeZone, now, 0)
		if got != want {
			gotTime := time.Unix(got, 0).Format(time.RFC3339Nano)
			t.Errorf("dateParamToEpoch(%q, local)=\n%v (%s)\nwant\n%v (%s)", tt.input, got, gotTime, want, wantTime)
		}
	}
}

func TestDateParamToEpochTruncate(t *testing.T) {
	timeZone := time.Local
	//16 Aug 1994 15:30
	now := time.Date(1994, time.August, 16, 15, 30, 0, 100, timeZone)

	const shortForm = "15:04:05 2006-Jan-02"

	var tests = []struct {
		input  string
		output string
	}{
		{"midnight", "00:00:00 1994-Aug-16"},
		{"noon", "12:00:00 1994-Aug-16"},
		{"teatime", "16:00:00 1994-Aug-16"},
		{"tomorrow", "00:00:00 1994-Aug-17"},

		{"noon 08/12/94", "12:00:00 1994-Aug-12"},
		{"midnight 20060812", "00:00:00 2006-Aug-12"},
		{"noon tomorrow", "12:00:00 1994-Aug-17"},

		{"17:04 19940812", "17:04:00 1994-Aug-12"},
		{"-1day", "15:30:00 1994-Aug-15"},
		{"19940812", "00:00:00 1994-Aug-12"},

		{"midnight-10", "23:59:50 1994-Aug-15"},
		{"midnight-1s", "23:59:59 1994-Aug-15"},
		{"midnight-1day", "00:00:00 1994-Aug-15"},

		// truncate
		{"now-1", "15:29:59 1994-Aug-16"},
		{"now-45s", "15:29:15 1994-Aug-16"},
	}

	for _, tt := range tests {
		var (
			want     int64
			wantTime string
		)
		if tt.output != "" {
			ts, err := time.ParseInLocation(shortForm, tt.output, timeZone)
			if err != nil {
				t.Fatalf("error parsing time: %q: %v", tt.output, err)
			}
			want = int64(ts.Unix())
			wantTime = ts.Format(time.RFC3339Nano)
		}

		got := DateParamToEpoch(tt.input, timeZone, now, 10*time.Second)
		if got != want {
			gotTime := time.Unix(got, 0).Format(time.RFC3339Nano)
			t.Errorf("dateParamToEpoch(%q, local)=\n%v (%s)\nwant\n%v (%s)", tt.input, got, gotTime, want, wantTime)
		}
	}
}
