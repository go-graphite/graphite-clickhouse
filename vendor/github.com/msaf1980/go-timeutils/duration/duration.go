package duration

import (
	"strconv"
	"time"
)

func fmtSec(buf []byte, v int64) []byte {
	if v == 0 {
		return buf
	}

	n := v / int64(time.Second)
	buf = strconv.AppendInt(buf, n, 10)
	v -= n * int64(time.Second)

	var prec int64 = 1e8
	if v > 0 {
		buf = append(buf, '.')
		for v > 0 {
			n = v / prec
			buf = append(buf, byte(n)+'0')
			v -= n * prec
			prec /= 10
		}
	}

	return buf
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
		buf = fmtSec(buf, u)
		buf = append(buf, "s"...)
	}
	return unsafeString(buf)
}
