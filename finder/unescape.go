package finder

import "strings"

func ishex(c byte) bool {
	switch {
	case '0' <= c && c <= '9':
		return true
	case 'a' <= c && c <= 'f':
		return true
	case 'A' <= c && c <= 'F':
		return true
	}

	return false
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}

	return 0
}

func isPercentEscape(s string, i int) bool {
	return i+2 < len(s) && ishex(s[i+1]) && ishex(s[i+2])
}

// unescape unescapes a string.
func unescape(s string) string {
	first := strings.IndexByte(s, '%')
	if first == -1 {
		return s
	}

	var t strings.Builder

	t.Grow(len(s))
	t.WriteString(s[:first])

LOOP:
	for i := first; i < len(s); i++ {
		switch s[i] {
		case '%':
			if len(s) < i+3 {
				t.WriteString(s[i:])
				break LOOP
			}
			if !isPercentEscape(s, i) {
				t.WriteString(s[i : i+3])
			} else {
				t.WriteByte(unhex(s[i+1])<<4 | unhex(s[i+2]))
			}
			i += 2
		default:
			t.WriteByte(s[i])
		}
	}

	return t.String()
}
