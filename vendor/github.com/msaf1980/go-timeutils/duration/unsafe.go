package duration

import "unsafe"

// unsafeString returns the string under byte buffer
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
