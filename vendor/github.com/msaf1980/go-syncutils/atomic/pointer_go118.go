//go:build go1.18
// +build go1.18

package atomic

import "fmt"

// String returns a human readable representation of a Pointer's underlying value.
func (p *Pointer[T]) String() string {
	return fmt.Sprint(p.Load())
}
