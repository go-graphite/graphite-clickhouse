package reverse

import (
	"bytes"
	"strings"
)

func String(path string) string {
	// don't reverse tagged path
	if strings.IndexByte(path, '?') >= 0 {
		return path
	}

	a := strings.Split(path, ".")

	l := len(a)
	for i := 0; i < l/2; i++ {
		a[i], a[l-i-1] = a[l-i-1], a[i]
	}

	return strings.Join(a, ".")
}

func reverse(m []byte) {
	i := 0
	j := len(m) - 1
	for i < j {
		m[i], m[j] = m[j], m[i]
		i++
		j--
	}
}

func Inplace(path []byte) {
	if bytes.IndexByte(path, '?') >= 0 {
		return
	}

	reverse(path)

	var a, b int
	l := len(path)
	for b = 0; b < l; b++ {
		if path[b] == '.' {
			reverse(path[a:b])
			a = b + 1
		}
	}
	reverse(path[a:b])
}

func Bytes(path []byte) []byte {
	// @TODO: test
	// don't reverse tagged path
	if bytes.IndexByte(path, '?') >= 0 {
		return path
	}
	r := make([]byte, len(path))
	copy(r, path)
	Inplace(r)

	return r
}
