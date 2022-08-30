package dry

import (
	"reflect"
	"unsafe"
)

// UnsafeString returns string object from byte slice without copying
func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// UnsafeStringBytes returns the string bytes
func UnsafeStringBytes(s *string) []byte {
	return *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(s))))
}
