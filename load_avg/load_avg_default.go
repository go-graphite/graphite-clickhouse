//go:build !linux
// +build !linux

package load_avg

func Normalized() (float64, error) {
	return 0, nil
}

func CpuCount() (uint64, error) {
	return 0, nil
}
