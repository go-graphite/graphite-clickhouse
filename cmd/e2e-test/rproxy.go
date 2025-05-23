package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lomik/graphite-clickhouse/pkg/dry"
)

type AtomicDuration struct {
	val int64
}

func (d *AtomicDuration) Store(duration time.Duration) {
	atomic.StoreInt64(&d.val, duration.Nanoseconds())
}

func (d *AtomicDuration) Load() time.Duration {
	return time.Duration(atomic.LoadInt64(&d.val))
}

func (d *AtomicDuration) MarshalText() ([]byte, error) {
	s := d.Load().String()
	return dry.UnsafeStringBytes(&s), nil
}

func (d *AtomicDuration) UnmarshalText(b []byte) error {
	val, err := time.ParseDuration(dry.UnsafeString(b))
	if err != nil {
		return err
	}

	d.Store(val)

	return nil
}

type HttpReverseProxy struct {
	Delay               AtomicDuration `toml:"delay"`
	BreakWithStatusCode int64          `toml:"break_with_status_code"`

	srv    *httptest.Server
	remote *url.URL
	wg     sync.WaitGroup
}

func (p *HttpReverseProxy) Start(remoteURL string) (err error) {
	if p.srv != nil {
		err = errors.New("reverse proxy already started")
		return
	}

	if p.BreakWithStatusCode < 0 {
		p.BreakWithStatusCode = 0
	}

	if p.remote, err = url.Parse(remoteURL); err != nil {
		err = errors.New("reverse proxy already started")
		return
	}

	p.srv = httptest.NewUnstartedServer(p)

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		p.srv.Start()
	}()

	return
}

func (p *HttpReverseProxy) Stop() {
	if p.srv == nil {
		return
	}

	p.srv.CloseClientConnections()
	p.srv.Close()
	p.wg.Wait()
	p.srv = nil
}

func (p *HttpReverseProxy) URL() string {
	return p.srv.URL
}

func (p *HttpReverseProxy) SetDelay(delay time.Duration) {
	p.Delay.Store(delay)
}

func (p *HttpReverseProxy) GetDelay() time.Duration {
	return p.Delay.Load()
}

func (p *HttpReverseProxy) SetBreakStatusCode(statusCode int) {
	atomic.StoreInt64(&p.BreakWithStatusCode, int64(statusCode))
}

func (p *HttpReverseProxy) GetBreakStatusCode() int {
	return int(atomic.LoadInt64(&p.BreakWithStatusCode))
}

func (p *HttpReverseProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.Host = p.remote.Host

	delay := p.GetDelay()
	if delay != 0 {
		time.Sleep(delay)
	}

	breakWithStatusCode := p.GetBreakStatusCode()
	if breakWithStatusCode != 0 {
		http.Error(w, "", breakWithStatusCode)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(p.remote)
	proxy.ServeHTTP(w, r)
}
