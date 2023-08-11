package atomic

//go:generate bin/gen-atomicwrapper -name=Bool -type=bool -wrapped=Uint32 -pack=boolToInt -unpack=truthy -cas -swap -json -file=bool.go

func truthy(n uint32) bool {
	return n == 1
}

func boolToUint32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
