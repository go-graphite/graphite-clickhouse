package scope

import (
	"context"
	"net/http"

	"github.com/lomik/graphite-clickhouse/helper/headers"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

var (
	CarbonapiUUIDName  = "carbonapi_uuid"
	RequestHeadersName = "request_headers"
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

// Logger returns zap.Logger instance
func LoggerWithHeaders(ctx context.Context, r *http.Request, headersToLog []string) *zap.Logger {
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

	carbonapiUUID := r.Header.Get("X-Ctx-Carbonapi-Uuid")
	if carbonapiUUID != "" {
		zapLogger = zapLogger.With(zap.String("carbonapi_uuid", carbonapiUUID))
	}
	requestHeaders := headers.GetHeaders(&r.Header, headersToLog)
	if len(requestHeaders) > 0 {
		zapLogger = zapLogger.With(zap.Any("request_headers", requestHeaders))
	}

	return zapLogger
}

// WithLogger ...
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return With(ctx, "logger", logger)
}
