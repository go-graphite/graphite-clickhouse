package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lomik/zapwriter"

	"go.uber.org/zap"
)

var ErrUvarintRead = errors.New("ReadUvarint: Malformed array")
var ErrUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var ErrClickHouseResponse = errors.New("Malformed response from clickhouse")

func formatSQL(q string) string {
	s := strings.Split(q, "\n")
	for i := 0; i < len(s); i++ {
		s[i] = strings.TrimSpace(s[i])
	}

	return strings.Join(s, " ")
}

func Escape(s string) string {
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `'`, `\'`, -1)
	return s
}

func Query(ctx context.Context, dsn string, query string, timeout time.Duration) ([]byte, error) {
	return Post(ctx, dsn, query, nil, timeout)
}

func Post(ctx context.Context, dsn string, query string, postBody io.Reader, timeout time.Duration) ([]byte, error) {
	return do(ctx, dsn, query, postBody, false, timeout)
}

func PostGzip(ctx context.Context, dsn string, query string, postBody io.Reader, timeout time.Duration) ([]byte, error) {
	return do(ctx, dsn, query, postBody, true, timeout)
}

func do(ctx context.Context, dsn string, query string, postBody io.Reader, gzip bool, timeout time.Duration) (body []byte, err error) {
	start := time.Now()

	queryForLogger := query
	if len(queryForLogger) > 500 {
		queryForLogger = queryForLogger[:395] + "<...>" + queryForLogger[len(queryForLogger)-100:]
	}
	logger := zapwriter.Logger("query").With(zap.String("query", formatSQL(queryForLogger)))

	defer func() {
		d := time.Since(start)
		log := logger.With(
			zap.Duration("time", d),
		)
		// fmt.Println(time.Since(start), formatSQL(queryForLogger))
		if err != nil {
			log.Error("query", zap.Error(err))
		} else {
			log.Info("query")
		}
	}()

	p, err := url.Parse(dsn)
	if err != nil {
		return
	}

	if postBody != nil {
		q := p.Query()
		q.Set("query", query)
		p.RawQuery = q.Encode()
	} else {
		postBody = strings.NewReader(query)
	}

	url := p.String()

	req, err := http.NewRequest("POST", url, postBody)
	if err != nil {
		return
	}

	if gzip {
		req.Header.Add("Content-Encoding", "gzip")
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, _ = ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		err = fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
		return
	}

	return
}

func ReadUvarint(array []byte) (uint64, int, error) {
	var x uint64
	var s uint
	l := len(array) - 1
	for i := 0; ; i++ {
		if i > l {
			return x, i + 1, ErrUvarintRead
		}
		if array[i] < 0x80 {
			if i > 9 || i == 9 && array[i] > 1 {
				return x, i + 1, ErrUvarintOverflow
			}
			return x | uint64(array[i])<<s, i + 1, nil
		}
		x |= uint64(array[i]&0x7f) << s
		s += 7
	}
}
