package g2gcounters

import (
	"strconv"
	"sync/atomic"

	"github.com/msaf1980/g2g/pkg/expvars"
)

// Int is a 64-bit integer variable that satisfies the Var interface.
type UInt struct {
	i uint64
}

func NewUInt(name string) *UInt {
	v := new(UInt)
	expvars.Publish(name, v)
	return v
}

func (v *UInt) Value() uint64 {
	return atomic.LoadUint64(&v.i)
}

func (v *UInt) String() string {
	return strconv.FormatUint(atomic.LoadUint64(&v.i), 10)
}

func (v *UInt) Add(delta uint64) {
	atomic.AddUint64(&v.i, delta)
}

func (v *UInt) Set(value uint64) {
	atomic.StoreUint64(&v.i, value)
}
