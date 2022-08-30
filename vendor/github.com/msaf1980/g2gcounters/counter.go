package g2gcounters

import (
	"strconv"
	"sync/atomic"

	"github.com/msaf1980/g2g/pkg/expvars"
)

// Int is a 64-bit integer variable that satisfies the Var interface.
type Counter struct {
	i int64
}

func NewCounter(name string) *Counter {
	v := new(Counter)
	expvars.Publish(name, v)
	return v
}

func (v *Counter) Value() int64 {
	return atomic.SwapInt64(&v.i, 0)
}

func (v *Counter) String() string {
	return strconv.FormatInt(v.Value(), 10)
}

func (v *Counter) Add(delta int64) {
	atomic.AddInt64(&v.i, delta)
}

func (v *Counter) Incr() {
	atomic.AddInt64(&v.i, 1)
}
