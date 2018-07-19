package clickhouse

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/version"
	"github.com/lomik/zapwriter"

	"go.uber.org/zap"
)

var ErrUvarintRead = errors.New("ReadUvarint: Malformed array")
var ErrUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var ErrClickHouseResponse = errors.New("Malformed response from clickhouse")

type Options struct {
	Timeout        time.Duration
	ConnectTimeout time.Duration
}

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

func Query(ctx context.Context, dsn string, query string, table string, opts Options) ([]byte, error) {
	return Post(ctx, dsn, query, table, nil, opts)
}

func Post(ctx context.Context, dsn string, query string, table string, postBody io.Reader, opts Options) ([]byte, error) {
	return do(ctx, dsn, query, table, postBody, false, opts)
}

func PostGzip(ctx context.Context, dsn string, query string, table string, postBody io.Reader, opts Options) ([]byte, error) {
	return do(ctx, dsn, query, table, postBody, true, opts)
}

func Reader(ctx context.Context, dsn string, query string, table string, opts Options) (io.ReadCloser, error) {
	return reader(ctx, dsn, query, table, nil, false, opts)
}

func reader(ctx context.Context, dsn string, query string, table string, postBody io.Reader, gzip bool, opts Options) (bodyReader io.ReadCloser, err error) {
	start := time.Now()

	var requestID string
	if value, ok := ctx.Value("requestID").(string); ok {
		requestID = value
	}

	queryForLogger := query
	if len(queryForLogger) > 500 {
		queryForLogger = queryForLogger[:395] + "<...>" + queryForLogger[len(queryForLogger)-100:]
	}
	logger := zapwriter.Logger("query").With(zap.String("query", formatSQL(queryForLogger)), zap.String("request_id", requestID))

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

	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], rand.Uint64())
	queryID := fmt.Sprintf("%x", b)

	q := p.Query()
	q.Set("query_id", fmt.Sprintf("%s::%s", requestID, queryID))
	p.RawQuery = q.Encode()

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

	req.Header.Add("User-Agent", fmt.Sprintf("graphite-clickhouse/%s (table:%s)", version.Version, table))

	if gzip {
		req.Header.Add("Content-Encoding", "gzip")
	}

	client := &http.Client{Timeout: opts.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err = fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
		return
	}

	bodyReader = resp.Body
	return
}

func do(ctx context.Context, dsn string, query string, table string, postBody io.Reader, gzip bool, opts Options) ([]byte, error) {
	bodyReader, err := reader(ctx, dsn, query, table, postBody, gzip, opts)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(bodyReader)
	bodyReader.Close()
	if err != nil {
		return nil, err
	}

	return body, nil
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
