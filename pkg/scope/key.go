package scope

import (
	"context"
	"fmt"
)

// key is type for context.Value keys
type scopeKey string

// With returns a copy of parent in which the value associated with key is val.
func With(ctx context.Context, key string, value interface{}) context.Context {
	return context.WithValue(ctx, scopeKey(key), value)
}

// String returns the string value associated with this context for key
func String(ctx context.Context, key string) string {
	if value, ok := ctx.Value(scopeKey(key)).(string); ok {
		return value
	}

	return ""
}

// Bool returns the true if particular key of the context is set
func Bool(ctx context.Context, key string) bool {
	if _, ok := ctx.Value(scopeKey(key)).(bool); ok {
		return true
	}

	return false
}

// WithRequestID ...
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return With(ctx, "requestID", requestID)
}

// RequestID ...
func RequestID(ctx context.Context) string {
	return String(ctx, "requestID")
}

// WithTable ...
func WithTable(ctx context.Context, table string) context.Context {
	return With(ctx, "table", table)
}

// Table ...
func Table(ctx context.Context) string {
	return String(ctx, "table")
}

// WithDebug returns the context with debug-name
func WithDebug(ctx context.Context, name string) context.Context {
	return With(ctx, "debug-"+name, true)
}

// Debug returns true if debug-name should be enabled
func Debug(ctx context.Context, name string) bool {
	return Bool(ctx, "debug-"+name)
}

// ClickhouseUserAgent ...
func ClickhouseUserAgent(ctx context.Context) string {
	grafana := Grafana(ctx)
	if grafana != "" {
		return fmt.Sprintf("Graphite-Clickhouse/%s (table:%s) Grafana(%s)", Version, Table(ctx), grafana)
	}

	return fmt.Sprintf("Graphite-Clickhouse/%s (table:%s)", Version, Table(ctx))
}
