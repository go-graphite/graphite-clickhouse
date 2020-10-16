package scope

import (
	"context"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Logger returns zap.Logger instance
func Logger(ctx context.Context) *zap.Logger {
	logger := ctx.Value(scopeKey("logger"))
	var zapLogger *zap.Logger
	if logger != nil {
		if zl, ok := logger.(*zap.Logger); ok {
			zapLogger = zl
			return zapLogger
		}
	}
	if zapLogger == nil {
		zapLogger = zapwriter.Default()
	}

	requestId := RequestID(ctx)
	if requestId != "" {
		zapLogger = zapLogger.With(zap.String("request_id", requestId))
	}

	return zapLogger
}

// WithLogger ...
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return With(ctx, "logger", logger)
}
