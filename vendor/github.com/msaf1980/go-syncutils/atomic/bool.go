package atomic

import (
	"strconv"
	"sync/atomic"
)

// Bool is an atomic type-safe wrapper for bool values.
type Bool struct {
	_ noCopy

	v uint32
}

// NewBool creates a new Bool.
func NewBool(val bool) *Bool {
	return &Bool{v: boolToUint32(val)}
}

// Load atomically loads the wrapped bool.
func (x *Bool) Load() bool {
	return truthy(atomic.LoadUint32(&x.v))
}

// Store atomically stores the passed bool.
func (x *Bool) Store(val bool) {
	atomic.StoreUint32(&x.v, boolToUint32(val))
}

// CompareAndSwap is an atomic compare-and-swap for bool values.
func (x *Bool) CompareAndSwap(old, new bool) (swapped bool) {
	return atomic.CompareAndSwapUint32(&x.v, boolToUint32(old), boolToUint32(new))
}

// Swap atomically stores the given bool and returns the old value.
func (x *Bool) Swap(val bool) (old bool) {
	return truthy(atomic.SwapUint32(&x.v, boolToUint32(val)))
}

// Toggle atomically negates the Boolean and returns the previous value.
func (b *Bool) Toggle() (old bool) {
	for {
		old := b.Load()
		if b.CompareAndSwap(old, !old) {
			return old
		}
	}
}

// String encodes the wrapped value as a string.
func (b *Bool) String() string {
	return strconv.FormatBool(b.Load())
}
