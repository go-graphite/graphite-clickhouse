package g2g

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/msaf1980/g2g/pkg/expvars"
)

const (
	networkSeparator = "://"
)

// Graphite represents a Graphite server. You Register expvars
// in this struct, which will be published to the server on a
// regular interval.
type Graphite struct {
	network        string
	endpoint       string
	interval       time.Duration
	timeout        time.Duration
	connection     net.Conn
	batchSize      int
	batchBuf       *bytes.Buffer
	vars           map[string]expvars.Var
	mvars          map[string]expvars.MVar
	registrations  chan namedVar
	mregistrations chan namedMVar
	shutdown       chan chan bool
}

// A namedVar couples an expvar (interface) with an "external" name.
type namedVar struct {
	name string
	v    expvars.Var
}

// A mnamedVar couples an expvar (interface) with an "external" name.
type namedMVar struct {
	name string
	v    expvars.MVar
}

// splitEndpoint splits the provided endpoint string into its network and address
// parts. It will default to 'tcp' network to ensure backward compatibility when
// the endpoint is not prefixed with a network:// part.
func splitEndpoint(endpoint string) (string, string) {
	network := "tcp"
	idx := strings.Index(endpoint, networkSeparator)
	if idx != -1 {
		network, endpoint = endpoint[:idx], endpoint[idx+len(networkSeparator):]
	}
	return network, endpoint
}

// NewGraphite returns a Graphite structure with no active/registered
// variables being published.  The connection setup is lazy, e.g. it is
// done at the first metric submission.
// Endpoint should be of the format "network://host:port", eg. "tcp://stats:2003".
// Interval is the (best-effort) minimum duration between (sequential)
// publishments of Registered expvars. Timeout is per-publish-action.
func NewGraphite(endpoint string, interval, timeout time.Duration) *Graphite {
	return NewGraphiteBatch(endpoint, interval, timeout, 0)
}

// NewGraphiteBatch returns a Graphite structure with no active/registered
// variables being published.  The connection setup is lazy, e.g. it is
// done at the first metric submission. Write to connections are batch
// Endpoint should be of the format "network://host:port", eg. "tcp://stats:2003".
// Interval is the (best-effort) minimum duration between (sequential)
// publishments of Registered expvars. Timeout is per-publish-action.
func NewGraphiteBatch(endpoint string, interval, timeout time.Duration, batchSize int) *Graphite {
	network, endpoint := splitEndpoint(endpoint)
	g := &Graphite{
		network:        network,
		endpoint:       endpoint,
		interval:       interval,
		timeout:        timeout,
		connection:     nil,
		vars:           map[string]expvars.Var{},
		mvars:          map[string]expvars.MVar{},
		registrations:  make(chan namedVar),
		mregistrations: make(chan namedMVar),
		shutdown:       make(chan chan bool),
	}
	if batchSize > 1 {
		if batchSize < 512 {
			batchSize = 512
		}
		g.batchSize = batchSize
		g.batchBuf = &bytes.Buffer{}
		g.batchBuf.Grow(batchSize)
	} else {
		g.batchSize = 0
	}

	go g.loop()
	return g
}

// Register registers an expvar under the given name. (Roughly) every
// interval, the current value of the given expvar will be published to
// Graphite under the given name.
func (g *Graphite) Register(name string, v expvars.Var) {
	g.registrations <- namedVar{name, v}
}

// MRegister registers an multi-alue expvar under the given name. (Roughly) every
// interval, the current value of the given expvar will be published to
// Graphite under the given name.
func (g *Graphite) MRegister(name string, v expvars.MVar) {
	g.mregistrations <- namedMVar{name, v}
}

// Shutdown signals the Graphite structure to stop publishing
// Registered expvars.
func (g *Graphite) Shutdown() {
	q := make(chan bool)
	g.shutdown <- q
	<-q
}

func (g *Graphite) loop() {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case nv := <-g.registrations:
			g.vars[nv.name] = nv.v
		case nv := <-g.mregistrations:
			g.mvars[nv.name] = nv.v
		case <-ticker.C:
			g.postAll()
		case q := <-g.shutdown:
			if g.connection != nil {
				g.connection.Close()
				g.connection = nil
			}
			q <- true
			return
		}
	}
}

func (g *Graphite) flushBuf() (int, error) {
	if g.batchBuf.Len() > 0 {
		if g.connection == nil {
			if err := g.reconnect(); err != nil {
				return 0, err
			}
		}
		n, err := g.connection.Write(g.batchBuf.Bytes())
		if err != nil {
			// retry
			g.connection = nil
			time.Sleep(time.Second)
			if err := g.reconnect(); err != nil {
				return 0, err
			}
			n, err = g.connection.Write(g.batchBuf.Bytes())
		}

		return n, err
	} else {
		return 0, nil
	}
}

// postAll publishes all Registered expvars to the Graphite server.
func (g *Graphite) postAll() {
	if len(g.vars) == 0 && len(g.mvars) == 0 {
		return
	}

	if g.batchSize > 0 {
		g.batchBuf.Reset()
		for name, v := range g.vars {
			g.bufOne(name, v.String())
			if g.batchBuf.Len() >= g.batchSize {
				if n, err := g.flushBuf(); err != nil {
					g.connection = nil
					log.Printf("g2g: write: %s", err)
				} else if n != g.batchBuf.Len() {
					g.connection = nil
					log.Printf("g2g: short write: %d/%d", n, g.batchBuf.Len())
				}
				g.batchBuf.Reset()
			}
		}
		for name, mv := range g.mvars {
			for _, v := range mv.Strings() {
				g.bufOne(name+v.Name, v.V)
			}
			if g.batchBuf.Len() >= g.batchSize {
				if n, err := g.flushBuf(); err != nil {
					g.connection = nil
					log.Printf("g2g: write: %s", err)
				} else if n != g.batchBuf.Len() {
					g.connection = nil
					log.Printf("g2g: short write: %d/%d", n, g.batchBuf.Len())
				}
				g.batchBuf.Reset()
			}
		}

		if n, err := g.flushBuf(); err != nil {
			g.connection = nil
			log.Printf("g2g: write: %s", err)
		} else if n != g.batchBuf.Len() {
			g.connection = nil
			log.Printf("g2g: short write: %d/%d", n, g.batchBuf.Len())
		}
	} else {
		for name, v := range g.vars {
			if err := g.postOne(name, v.String()); err != nil {
				log.Printf("g2g: %s: %s", name, err)
			}
		}
		for name, mv := range g.mvars {
			for _, v := range mv.Strings() {
				if err := g.postOne(name+v.Name, v.V); err != nil {
					log.Printf("g2g: %s: %s", name, err)
				}
			}
		}
	}
}

// bufOne store the given name-value pair in send buffer.
func (g *Graphite) bufOne(name, value string) {
	g.batchBuf.WriteString(name)
	g.batchBuf.WriteString(" ")
	g.batchBuf.WriteString(value)
	g.batchBuf.WriteString(" ")
	g.batchBuf.WriteString(strconv.FormatInt(time.Now().Unix(), 10))
	g.batchBuf.WriteString("\n")
}

// postOne publishes the given name-value pair to the Graphite server.
// If the connection is broken, one reconnect attempt is made.
func (g *Graphite) postOne(name, value string) error {
	if g.connection == nil {
		if err := g.reconnect(); err != nil {
			return fmt.Errorf("failed; reconnect attempt: %s", err)
		}
	}
	deadline := time.Now().Add(g.timeout)
	if err := g.connection.SetWriteDeadline(deadline); err != nil {
		g.connection = nil
		return fmt.Errorf("SetWriteDeadline: %s", err)
	}
	b := []byte(fmt.Sprintf("%s %s %d\n", name, value, time.Now().Unix()))
	if n, err := g.connection.Write(b); err != nil {
		g.connection = nil
		return fmt.Errorf("write: %s", err)
	} else if n != len(b) {
		g.connection = nil
		return fmt.Errorf("%s = %v: short write: %d/%d", name, value, n, len(b))
	}
	return nil
}

// reconnect attempts to (re-)establish a TCP connection to the Graphite server.
func (g *Graphite) reconnect() error {
	conn, err := net.Dial(g.network, g.endpoint)
	if err != nil {
		return err
	}
	g.connection = conn
	return nil
}
