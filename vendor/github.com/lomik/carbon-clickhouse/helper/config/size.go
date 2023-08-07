package config

import (
	"fmt"
	"strconv"
	"strings"
)

type Size int64

func (u *Size) UnmarshalText(text []byte) error {
	var err error
	var s int64

	value := strings.ToLower(string(text))
	last := len(value) - 1
	suffix := value[last]
	switch suffix {
	case 'k':
		s, err = strconv.ParseInt(value[0:last], 10, 64)
		s *= 1024
	case 'm':
		s, err = strconv.ParseInt(value[0:last], 10, 64)
		s *= 1024 * 1024
	case 'g':
		s, err = strconv.ParseInt(value[0:last], 10, 64)
		s *= 1024 * 1024 * 1024
	default:
		s, err = strconv.ParseInt(value, 10, 64)
	}

	if s < 0 {
		err = fmt.Errorf("size must be greater than 0")
	}
	*u = Size(s)
	return err
}

func (u *Size) Value() int64 {
	return int64(*u)
}
