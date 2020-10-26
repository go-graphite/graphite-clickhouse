package scope

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/http"
	"regexp"
)

var requestIdRegexp *regexp.Regexp = regexp.MustCompile("^[a-zA-Z0-9_.-]+$")
var passHeaders = []string{
	"X-Dashboard-Id",
	"X-Grafana-Org-Id",
	"X-Panel-Id",
}

func HttpRequest(r *http.Request) *http.Request {
	requestID := r.Header.Get("X-Request-Id")
	if requestID == "" || !requestIdRegexp.MatchString(requestID) {
		var b [16]byte
		binary.LittleEndian.PutUint64(b[:], rand.Uint64())
		binary.LittleEndian.PutUint64(b[8:], rand.Uint64())
		requestID = fmt.Sprintf("%x", b)
	}

	ctx := r.Context()
	ctx = WithRequestID(ctx, requestID)

	if d := r.Header.Get("X-Gch-Debug-External-Data"); d != "" {
		ctx = WithDebug(ctx, "ExternalData")
	}

	for _, h := range passHeaders {
		hv := r.Header.Get(h)
		if hv != "" {
			ctx = With(ctx, h, hv)
		}
	}

	return r.WithContext(ctx)
}

func Grafana(ctx context.Context) string {
	o, d, p := String(ctx, "X-Grafana-Org-Id"), String(ctx, "X-Dashboard-Id"), String(ctx, "X-Panel-Id")
	if o != "" || d != "" || p != "" {
		return fmt.Sprintf("Org:%s; Dashboard:%s; Panel:%s", o, d, p)
	}
	return ""
}
