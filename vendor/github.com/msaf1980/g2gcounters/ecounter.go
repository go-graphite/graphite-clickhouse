package g2gcounters

import (
	"strconv"
	"sync/atomic"

	"github.com/msaf1980/g2g/pkg/expvars"
)

// Int is a 64-bit integer variable that satisfies the MVar interface. 0 is not sent
type ECounter struct {
	i int64
}

func NewECounter(name string) *ECounter {
	v := new(ECounter)
	expvars.MPublish(name, v)
	return v
}

func (v *ECounter) Value() int64 {
	return atomic.SwapInt64(&v.i, 0)
}

func (v *ECounter) Strings() []expvars.MValue {
	n := v.Value()
	if n == 0 {
		return []expvars.MValue{}
	}
	return []expvars.MValue{
		{Name: "", V: strconv.FormatInt(n, 10)},
	}
}

func (v *ECounter) Add(delta int64) {
	atomic.AddInt64(&v.i, delta)
}

func (v *ECounter) Incr() {
	atomic.AddInt64(&v.i, 1)
}
