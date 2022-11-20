//go:build !noprom
// +build !noprom

package prometheus

import (
	"go.uber.org/zap"
)

type errorLevel interface {
	String() string
}

type logger struct {
	z *zap.Logger
}

func (l *logger) Log(keyvals ...interface{}) error {
	lg := l.z
	var msg string
	var level errorLevel

	for i := 1; i < len(keyvals); i += 2 {
		keyObj := keyvals[i-1]
		keyStr, ok := keyObj.(string)
		if !ok {
			l.z.Error("can't handle log, wrong key", zap.Any("keyvals", keyvals))
			return nil
		}

		if keyStr == "level" {
			level, ok = keyvals[i].(errorLevel)
			if !ok {
				l.z.Error("can't handle log, wrong level", zap.Any("keyvals", keyvals))
				return nil
			}
			continue
		}

		if keyStr == "msg" {
			msg, ok = keyvals[i].(string)
			if !ok {
				l.z.Error("can't handle log, wrong msg", zap.Any("keyvals", keyvals))
				return nil
			}
			continue
		}

		lg = lg.With(zap.Any(keyStr, keyvals[i]))
	}

	switch level.String() {
	case "debug":
		lg.Debug(msg)
	case "info":
		lg.Info(msg)
	case "warn":
		lg.Warn(msg)
	case "error":
		lg.Error(msg)
	default:
		l.z.Error("can't handle log, unknown level", zap.Any("keyvals", keyvals))
		return nil
	}
	return nil
}
