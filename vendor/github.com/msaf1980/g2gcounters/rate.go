package g2gcounters

import (
	"sync/atomic"
	"time"

	"github.com/msaf1980/g2g/pkg/expvars"
)

// Rate is a rate variable that satisfies the Var interface (reset on get value).
type Rate struct {
	i    int64
	last int64
}

func NewRate(name string) *Rate {
	v := new(Rate)
	v.last = time.Now().UnixNano()

	expvars.Publish(name, v)

	return v
}

func (v *Rate) Value() float64 {
	now := time.Now().UnixNano()
	prev := v.last
	v.last = now
	count := atomic.SwapInt64(&v.i, 0)
	if count == 0 {
		return 0.0
	}
	durations := now - prev

	return float64(count) * (1000000000.0 / float64(durations))
}

func (v *Rate) String() string {
	return expvars.RoundFloat(v.Value())
}

func (v *Rate) Add(delta int64) {
	atomic.AddInt64(&v.i, delta)
}

func (v *Rate) Incr() {
	atomic.AddInt64(&v.i, 1)
}
