//go:build go1.18 && !go1.19
// +build go1.18,!go1.19

package atomic

import "unsafe"

type Pointer[T any] struct {
	// Mention *T in a field to disallow conversion between Pointer types.
	// See go.dev/issue/56603 for more details.
	// Use *T, not T, to avoid spurious recursive type definition errors.
	_ [0]*T

	_ noCopy

	p UnsafePointer
}

// NewPointer creates a new Pointer.
func NewPointer[T any](v *T) *Pointer[T] {
	var p Pointer[T]
	if v != nil {
		p.p.Store(unsafe.Pointer(v))
	}
	return &p
}

// Load atomically loads the wrapped value.
func (p *Pointer[T]) Load() *T {
	return (*T)(p.p.Load())
}

// Store atomically stores the passed value.
func (p *Pointer[T]) Store(val *T) {
	p.p.Store(unsafe.Pointer(val))
}

// Swap atomically swaps the wrapped pointer and returns the old value.
func (p *Pointer[T]) Swap(val *T) (old *T) {
	return (*T)(p.p.Swap(unsafe.Pointer(val)))
}

// CompareAndSwap is an atomic compare-and-swap.
func (p *Pointer[T]) CompareAndSwap(old, new *T) (swapped bool) {
	return p.p.CompareAndSwap(unsafe.Pointer(old), unsafe.Pointer(new))
}
