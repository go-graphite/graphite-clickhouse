package expand

import (
	"go/token"
	"strconv"
	"strings"
)

func ExpandTimestamp(fs *token.FileSet, s string, replace map[string]string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	for k, v := range replace {
		s = strings.ReplaceAll(s, k, v)
	}
	s = strings.Trim(s, "")
	if s == "" {
		return 0, nil
	}
	var sign int64
	indx := strings.LastIndex(s, "+")
	if indx == -1 {
		if indx = strings.LastIndex(s, "-"); indx == -1 {
			indx = len(s)
		} else {
			sign = -1
		}
	} else {
		sign = 1
	}

	n, err := strconv.ParseInt(s[:indx], 10, 32)
	if err != nil {
		return 0, err
	}
	if sign == 0 {
		return n, nil
	}

	var add int64
	switch s[len(s)-1] {
	case 's':
		if add, err = strconv.ParseInt(s[indx+1:len(s)-1], 10, 32); err != nil {
			return 0, err
		}
	case 'm':
		if add, err = strconv.ParseInt(s[indx+1:len(s)-1], 10, 32); err != nil {
			return 0, err
		}
		add *= 60
	case 'h':
		if add, err = strconv.ParseInt(s[indx+1:len(s)-1], 10, 32); err != nil {
			return 0, err
		}
		add *= 3600
	case 'd':
		if add, err = strconv.ParseInt(s[indx+1:len(s)-1], 10, 32); err != nil {
			return 0, err
		}
		add *= 3600 * 24
	case 'M':
		if add, err = strconv.ParseInt(s[indx+1:len(s)-1], 10, 32); err != nil {
			return 0, err
		}
		add *= 3600 * 24 * 30
	default:
		if add, err = strconv.ParseInt(s[indx+1:], 10, 32); err != nil {
			return 0, err
		}

	}
	return n + sign*add, nil
}
