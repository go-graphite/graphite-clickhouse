//go:build !linux
// +build !linux

package load_avg

import (
	"os"
	"strings"
	"syscall"

	"github.com/msaf1980/go-stringutils"
)

func Normalized() (float64, error) {
	return 0, nil
}

func CpuCount() (uint64, error) {
	return 0, nil
}
