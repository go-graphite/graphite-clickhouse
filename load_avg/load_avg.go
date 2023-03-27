package load_avg

import (
	"os"
	"strings"
	"syscall"

	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/go-syncutils/atomic"
)

var (
	loadAvgStore atomic.Float64
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
	load1 := float64(info.Loads[0]) / float64(1<<si_load_shift) / float64(cpus)
	return load1, nil
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

func Load() float64 {
	return loadAvgStore.Load()
}

func Store(f float64) {
	loadAvgStore.Store(f)
}
