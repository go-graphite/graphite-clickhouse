package duration

import (
	"strconv"
	"time"
)

func fmtSub(buf []byte, v int64, shift int64) []byte {
	if v == 0 {
		return buf
	}

	if shift <= 0 {
		shift = 1
	}
	n := v / shift
	buf = strconv.AppendInt(buf, n, 10)
	v -= n * shift
	shift /= 10

	if v > 0 {
		buf = append(buf, '.')
		for v > 0 && shift > 0 {
			n = v / shift
			buf = append(buf, byte(n)+'0')
			v -= n * shift
			shift /= 10
		}
	}

	return buf
}

// MustParse like time.ParseDuration, but panic on error
func MustParse(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil {
		return d
	} else {
		panic(err)
	}
}

// String returns a string representing the duration in the form "72h3m0.5s".
// Zero units are omitted. As a special case, durations less than one
// second format use a smaller unit (milli-, micro-, or nanoseconds) to ensure
// that the leading digit is non-zero. The zero duration formats as 0s.
func String(d time.Duration) string {
	// Largest time is 2540400h10m10.000000000s
	u := int64(d)
	if u == 0 {
		return "0s"
	}
	buf := make([]byte, 0, 32)
	if d < 0 {
		u = -u
		buf = append(buf, '-')
	}

	if u < int64(time.Microsecond) {
		// print nanoseconds
		buf = fmtSub(buf, u, 1)
		buf = append(buf, "ns"...)
	} else if u < int64(time.Millisecond) {
		// print microseconds
		buf = fmtSub(buf, u, 1e3)
		buf = append(buf, "µs"...)
	} else if u < int64(time.Second) {
		// print milliseconds
		buf = fmtSub(buf, u, 1e6)
		buf = append(buf, "ms"...)
	} else {
		if u >= int64(time.Hour) {
			n := u / int64(time.Hour)
			buf = strconv.AppendInt(buf, n, 10)
			buf = append(buf, 'h')
			u -= n * int64(time.Hour)
		}
		if u >= int64(time.Minute) {
			n := u / int64(time.Minute)
			buf = strconv.AppendInt(buf, n, 10)
			buf = append(buf, 'm')
			u -= n * int64(time.Minute)
		}

		// second part
		if u == 0 {
			if len(buf) == 0 {
				buf = append(buf, "0s"...)
			}
		} else {
			// print seconds
			buf = fmtSub(buf, u, 1e9)
			buf = append(buf, "s"...)
		}
	}
	return unsafeString(buf)
}
