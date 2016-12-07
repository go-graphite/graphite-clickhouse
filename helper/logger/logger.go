package backend

import (
	"context"

	"github.com/uber-go/zap"
)

func FromContext(ctx context.Context) zap.Logger {
	logger := ctx.Value("logger")
	if logger == nil {
		return zap.New(zap.NullEncoder())
	}
	if zapLogger, ok := logger.(zap.Logger); ok {
		return zapLogger
	}

	return zap.New(zap.NullEncoder())
}
