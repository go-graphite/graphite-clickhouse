package clickhouse

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/zapwriter"

	"go.uber.org/zap"
)

type ErrDataParse struct {
	err  string
	data string
}

func NewErrDataParse(err string, data string) error {
	return &ErrDataParse{err, data}
}

func (e *ErrDataParse) Error() string {
	return fmt.Sprintf("%s: %s", e.err, e.data)
}

func (e *ErrDataParse) PrependDescription(test string) {
	e.data = test + e.data
}

type ErrorWithCode struct {
	err  string
	Code int // error code
}

func NewErrorWithCode(err string, code int) error {
	return &ErrorWithCode{err, code}
}

func (e *ErrorWithCode) Error() string { return e.err }

var ErrUvarintRead = errors.New("ReadUvarint: Malformed array")
var ErrUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var ErrClickHouseResponse = errors.New("Malformed response from clickhouse")

func HandleError(w http.ResponseWriter, err error) {
	netErr, ok := err.(net.Error)
	if ok {
		if netErr.Timeout() {
			http.Error(w, "Storage read timeout", http.StatusGatewayTimeout)
		} else if strings.HasSuffix(err.Error(), "connect: no route to host") ||
			strings.HasSuffix(err.Error(), "connect: connection refused") ||
			strings.HasSuffix(err.Error(), ": connection reset by peer") ||
			strings.HasPrefix(err.Error(), "dial tcp: lookup ") { // DNS lookup
			http.Error(w, "Storage error", http.StatusServiceUnavailable)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	errCode, ok := err.(*ErrorWithCode)
	if ok {
		if errCode.Code > 500 && errCode.Code < 512 {
			http.Error(w, errCode.Error(), errCode.Code)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	_, ok = err.(*ErrDataParse)
	if ok || strings.HasPrefix(err.Error(), "clickhouse response status 500: Code:") {
		if strings.Contains(err.Error(), ": Limit for ") {
			//logger.Info("limit", zap.Error(err))
			http.Error(w, "Storage read limit", http.StatusForbidden)
		} else if !ok && strings.HasPrefix(err.Error(), "clickhouse response status 500: Code: 170,") {
			// distributed table configuration error
			// clickhouse response status 500: Code: 170, e.displayText() = DB::Exception: Requested cluster 'cluster' not found
			http.Error(w, "Storage configuration error", http.StatusServiceUnavailable)
		}
	} else {
		//logger.Debug("query", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type Options struct {
	Timeout        time.Duration
	ConnectTimeout time.Duration
}

type loggedReader struct {
	reader   io.ReadCloser
	logger   *zap.Logger
	start    time.Time
	finished bool
	queryID  string
}

func (r *loggedReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && !r.finished {
		r.finished = true
		r.logger.Info("query", zap.String("query_id", r.queryID), zap.Duration("time", time.Since(r.start)))
	}
	return n, err
}

func (r *loggedReader) Close() error {
	err := r.reader.Close()
	if !r.finished {
		r.finished = true
		r.logger.Info("query", zap.String("query_id", r.queryID), zap.Duration("time", time.Since(r.start)))
	}
	return err
}

func formatSQL(q string) string {
	s := strings.Split(q, "\n")
	for i := 0; i < len(s); i++ {
		s[i] = strings.TrimSpace(s[i])
	}

	return strings.Join(s, " ")
}

func Query(ctx context.Context, dsn string, query string, opts Options) ([]byte, error) {
	return Post(ctx, dsn, query, nil, opts)
}

func Post(ctx context.Context, dsn string, query string, postBody io.Reader, opts Options) ([]byte, error) {
	return do(ctx, dsn, query, postBody, false, opts)
}

func PostGzip(ctx context.Context, dsn string, query string, postBody io.Reader, opts Options) ([]byte, error) {
	return do(ctx, dsn, query, postBody, true, opts)
}

func Reader(ctx context.Context, dsn string, query string, opts Options) (io.ReadCloser, error) {
	return reader(ctx, dsn, query, nil, false, opts)
}

func reader(ctx context.Context, dsn string, query string, postBody io.Reader, gzip bool, opts Options) (bodyReader io.ReadCloser, err error) {
	var chQueryID string

	start := time.Now()

	requestID := scope.RequestID(ctx)

	queryForLogger := query
	if len(queryForLogger) > 500 {
		queryForLogger = queryForLogger[:395] + "<...>" + queryForLogger[len(queryForLogger)-100:]
	}
	logger := zapwriter.Logger("query").With(zap.String("query", formatSQL(queryForLogger)), zap.String("request_id", requestID))

	defer func() {
		// fmt.Println(time.Since(start), formatSQL(queryForLogger))
		if err != nil {
			logger.Error("query", zap.Error(err), zap.Duration("time", time.Since(start)))
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

	req.Header.Add("User-Agent", scope.ClickhouseUserAgent(ctx))

	if gzip {
		req.Header.Add("Content-Encoding", "gzip")
	}

	client := &http.Client{
		Timeout: opts.Timeout,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: opts.ConnectTimeout,
			}).Dial,
			DisableKeepAlives: true,
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	// chproxy overwrite our query id. So read it again
	chQueryID = resp.Header.Get("X-ClickHouse-Query-Id")

	// check for return 5xx error, may be 502 code if clickhouse accesed via reverse proxy
	if resp.StatusCode > 500 && resp.StatusCode < 512 {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err = NewErrorWithCode(string(body), resp.StatusCode)
		return
	} else if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err = fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
		return
	}

	bodyReader = &loggedReader{
		reader:  resp.Body,
		logger:  logger,
		start:   start,
		queryID: chQueryID,
	}

	return
}

func do(ctx context.Context, dsn string, query string, postBody io.Reader, gzip bool, opts Options) ([]byte, error) {
	bodyReader, err := reader(ctx, dsn, query, postBody, gzip, opts)
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
