//go:build linux
// +build linux

package load_avg

import (
	"os"
	"strings"
	"syscall"

	"github.com/msaf1980/go-stringutils"
)

func Normalized() (float64, error) {
	var info syscall.Sysinfo_t
	err := syscall.Sysinfo(&info)
	if err != nil {
		return 0, err
	}

	cpus, err := CpuCount()
	if err != nil {
		return 0, err
	}

	const si_load_shift = 16
	load := float64(info.Loads[0]) / float64(1<<si_load_shift) / float64(cpus)
	return load, nil
}

func CpuCount() (uint64, error) {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return 0, err
	}
	s := stringutils.UnsafeString(b)

	cpus := strings.Count(s, "processor\t: ")

	return uint64(cpus), nil
}
