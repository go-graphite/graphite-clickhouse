package g2gcounters

import (
	"sync/atomic"
	"time"

	"github.com/msaf1980/g2g/pkg/expvars"
)

// ERate is a rate variable that satisfies the MVar interface (reset on get value). 0 is not sent
type ERate struct {
	i    int64
	last int64
}

func NewERate(name string) *ERate {
	v := new(ERate)
	v.last = time.Now().UnixNano()

	expvars.MPublish(name, v)

	return v
}

func (v *ERate) Value() (float64, bool) {
	now := time.Now().UnixNano()
	prev := v.last
	v.last = now
	count := atomic.SwapInt64(&v.i, 0)
	if count == 0 {
		return 0.0, true
	}
	durations := now - prev

	return float64(count) * (1000000000.0 / float64(durations)), false
}

func (v *ERate) Strings() []expvars.MValue {
	if r, zero := v.Value(); zero {
		return []expvars.MValue{}
	} else {
		return []expvars.MValue{
			{Name: "", V: expvars.RoundFloat(r)},
		}
	}
}

func (v *ERate) Add(delta int64) {
	atomic.AddInt64(&v.i, delta)
}

func (v *ERate) Incr() {
	atomic.AddInt64(&v.i, 1)
}
