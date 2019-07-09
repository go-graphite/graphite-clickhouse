package scope

import "context"

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
