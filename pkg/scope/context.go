package scope

import (
	"context"

	"go.uber.org/zap"
)

// Context wrapper for context.Context with chain constructor
type Context struct {
	context.Context
}

// New ...
func New(ctx context.Context) *Context {
	return &Context{ctx}
}

// With ...
func (c *Context) With(key string, value interface{}) *Context {
	return New(With(c.Context, key, value))
}

// WithRequestID ...
func (c *Context) WithRequestID(requestID string) *Context {
	return New(WithRequestID(c.Context, requestID))
}

// WithLogger ...
func (c *Context) WithLogger(logger *zap.Logger) *Context {
	return New(WithLogger(c.Context, logger))
}

// WithTable ...
func (c *Context) WithTable(table string) *Context {
	return New(WithTable(c.Context, table))
}
