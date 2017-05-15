package log

import (
	"context"

	"go.uber.org/zap"
)

func FromContext(ctx context.Context) *zap.Logger {
	logger := ctx.Value("logger")
	if logger == nil {
		return zap.NewNop()
	}
	if zapLogger, ok := logger.(*zap.Logger); ok {
		return zapLogger
	}

	return zap.NewNop()
}
