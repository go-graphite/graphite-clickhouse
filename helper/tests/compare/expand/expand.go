package expand

import (
	"go/token"
	"go/types"
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
	if tv, err := types.Eval(fs, nil, token.NoPos, s); err == nil {
		return strconv.ParseInt(tv.Value.String(), 10, 32)
	} else {
		return 0, err
	}
}
