package dry

import "unsafe"

// UnsafeString returns string object from byte slice without copying
func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
