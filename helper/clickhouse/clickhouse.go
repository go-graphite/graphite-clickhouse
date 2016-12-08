package clickhouse

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/uber-go/zap"

	"github.com/lomik/graphite-clickhouse/helper/log"
)

func formatSQL(q string) string {
	s := strings.Split(q, "\n")
	for i := 0; i < len(s); i++ {
		s[i] = strings.TrimSpace(s[i])
	}

	return strings.Join(s, " ")
}

func Query(ctx context.Context, dsn string, query string, timeout time.Duration) (body []byte, err error) {
	start := time.Now()

	logger := log.FromContext(ctx)

	queryForLogger := query
	if len(queryForLogger) > 500 {
		queryForLogger = queryForLogger[:495] + "<...>"
	}
	logger = logger.With(zap.String("query", formatSQL(queryForLogger)))

	defer func() {
		log := logger.With(zap.Duration("time_ns", time.Since(start)))

		if err != nil {
			log.Error("query", zap.Error(err))
		} else {
			log.Debug("query")
		}
	}()

	p, err := url.Parse(dsn)
	if err != nil {
		return
	}

	url := p.String()

	req, err := http.NewRequest("POST", url, strings.NewReader(query))
	if err != nil {
		return
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, _ = ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		// logrus.Errorf("[clickhouse] (time: %s, error: %s) %s", time.Now().Sub(start).String(), string(body), formatSQL(query))
		err = fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
		return
	}

	// logrus.Debugf("[clickhouse] (time: %s) %s", time.Now().Sub(start).String(), formatSQL(query))
	return
}
