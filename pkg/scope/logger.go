package scope

import (
	"context"

	"go.uber.org/zap"
)

// Logger returns zap.Logger instance
func Logger(ctx context.Context) *zap.Logger {
	logger := ctx.Value(scopeKey("logger"))
	if logger == nil {
		return zap.NewNop()
	}
	if zapLogger, ok := logger.(*zap.Logger); ok {
		return zapLogger
	}

	return zap.NewNop()
}

// WithLogger ...
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return With(ctx, "logger", logger)
}
