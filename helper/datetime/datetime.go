package datetime

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
)

var ErrBadTime = errors.New("bad time")

// parseTime parses a time and returns hours and minutes
func parseTime(s string) (hour, minute int, err error) {
	switch s {
	case "midnight":
		return 0, 0, nil
	case "noon":
		return 12, 0, nil
	case "teatime":
		return 16, 0, nil
	}

	parts := strings.Split(s, ":")

	if len(parts) != 2 {
		return 0, 0, ErrBadTime
	}

	hour, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, ErrBadTime
	}

	minute, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, ErrBadTime
	}

	return hour, minute, nil
}

var TimeFormats = []string{"20060102", "01/02/06"}

// DateParamToEpoch turns a passed string parameter into a unix epoch
func DateParamToEpoch(s string, tz *time.Location, now time.Time, truncate time.Duration) int64 {
	if s == "" {
		// return the default if nothing was passed
		return 0
	}

	// relative timestamp
	if s[0] == '-' {
		offset, err := parser.IntervalString(s, -1)
		if err != nil {
			return 0
		}

		return now.Add(time.Duration(offset) * time.Second).Unix()
	} else if s[0] == '+' {
		offset, err := parser.IntervalString(s, 1)
		if err != nil {
			return 0
		}

		return now.Add(time.Duration(offset) * time.Second).Unix()
	}

	switch s {
	case "now":
		return now.Unix()
	case "rnow":
		return TimeTruncate(now, truncate).Unix()
	case "midnight", "noon", "teatime":
		yy, mm, dd := now.Date()
		hh, min, _ := parseTime(s) // error ignored, we know it's valid
		dt := time.Date(yy, mm, dd, hh, min, 0, 0, tz)
		return dt.Unix()
	}

	sint, err := strconv.Atoi(s)
	// need to check that len(s) != 8 to avoid turning 20060102 into seconds
	if err == nil && len(s) != 8 {
		return int64(sint) // We got a timestamp so returning it
	}

	s = strings.Replace(s, "_", " ", 1) // Go can't parse _ in date strings

	var ts, ds string
	split := strings.Fields(s)

	var t time.Time
	switch {
	case len(split) == 1:
		delim := strings.IndexAny(s, "+-")
		if delim == -1 {
			ds = s
		} else {
			ds = s[:delim]
			ts = s[delim:]
			switch ds {
			case "now", "today":
				t = now
			case "rnow", "rtoday":
				t = TimeTruncate(now, truncate)
				// nothing
			case "midnight", "noon", "teatime":
				yy, mm, dd := now.Date()
				hh, min, _ := parseTime(s) // error ignored, we know it's valid
				t = time.Date(yy, mm, dd, hh, min, 0, 0, tz)
			case "yesterday":
				t = now.AddDate(0, 0, -1)
			case "tomorrow":
				t = now.AddDate(0, 0, 1)
			default:
				return 0
			}

			offset, err := parser.IntervalString(ts, 1)
			if err != nil {
				offset64, err := strconv.ParseInt(ts, 10, 32)
				if err != nil {
					return 0
				}
				offset = int32(offset64)
			}

			return t.Add(time.Duration(offset) * time.Second).Unix()
		}
	case len(split) == 2:
		ts, ds = split[0], split[1]
	case len(split) > 2:
		return 0
	}

dateStringSwitch:
	switch ds {
	case "now", "today":
		t = now
	case "rnow", "rtoday":
		t = TimeTruncate(now, truncate)
	case "midnight", "noon", "teatime":
		yy, mm, dd := now.Date()
		hh, min, _ := parseTime(s) // error ignored, we know it's valid
		t = time.Date(yy, mm, dd, hh, min, 0, 0, tz)
	case "yesterday":
		t = now.AddDate(0, 0, -1)
	case "ryesterday":
		t = TimeTruncate(now, truncate).AddDate(0, 0, -1)
	case "tomorrow":
		t = now.AddDate(0, 0, 1)
	case "rtomorrow":
		t = TimeTruncate(now, truncate).AddDate(0, 0, 1)
	default:
		for _, format := range TimeFormats {
			t, err = time.ParseInLocation(format, ds, tz)
			if err == nil {
				break dateStringSwitch
			}
		}

		return 0
	}

	var hour, minute int
	if ts != "" {
		hour, minute, _ = parseTime(ts)
		// defaults to hour=0, minute=0 on error, which is midnight, which is fine for now
	}

	yy, mm, dd := t.Date()
	t = time.Date(yy, mm, dd, hour, minute, 0, 0, tz)

	return t.Unix()
}

func Timezone(qtz string) (*time.Location, error) {
	if qtz == "" {
		qtz = "Local"
	}
	return time.LoadLocation(qtz)
}

func TimestampTruncate(ts int64, truncate time.Duration) int64 {
	if ts == 0 || truncate == 0 {
		return ts
	}
	tm := time.Unix(ts, 0).UTC()
	return tm.Truncate(truncate).UTC().Unix()
}

func TimeTruncate(tm time.Time, truncate time.Duration) time.Time {
	if truncate == 0 {
		return tm
	}
	return tm.Truncate(truncate)
}
