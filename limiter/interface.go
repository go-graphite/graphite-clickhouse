package limiter

import (
	"context"
	"errors"
)

var ErrTimeout = errors.New("timeout exceeded")
var ErrOverflow = errors.New("storage maximum queries exceeded")

type ServerLimiter interface {
	Capacity() int
	Enabled() bool
	TryEnter(ctx context.Context, s string) error
	Enter(ctx context.Context, s string) error
	Leave(ctx context.Context, s string)
	SendDuration(queueMs int64)
	Unregiter()
}
