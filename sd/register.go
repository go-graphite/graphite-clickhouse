package sd

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/load_avg"
	"github.com/lomik/graphite-clickhouse/sd/nginx"
	"github.com/lomik/graphite-clickhouse/sd/utils"
	"go.uber.org/zap"
)

var (
	// ctxMain, Stop               = context.WithCancel(context.Background())
	stop     chan struct{} = make(chan struct{}, 1)
	delay                  = time.Second * 10
	hostname string
)

type SD interface {
	// Update update node record
	Update(listenIP, listenPort string, dc []string, weight int64) error
	// Delete delete node record (with ip/port/dcs)
	Delete(ip, port string, dcs []string) error
	// Delete delete node record
	DeleteNode(node string) (err error)
	// Clear clear node record (all except with current listen IP/port)
	Clear(listenIP, listenPort string) error
	// Nodes return all registered nodes (for all hostnames in namespace)
	Nodes() (nodes []utils.KV, err error)
	// List return all registered nodes for hostname
	List() (nodes []string, err error)
	// Namespace return namespace
	Namespace() string
}

func New(cfg *config.Common, hostname string, logger *zap.Logger) (SD, error) {
	switch cfg.SDType {
	case config.SDNginx:
		sd := nginx.New(cfg.SD, cfg.SDNamespace, hostname, logger)
		return sd, nil
	default:
		return nil, errors.New("serive discovery type not registered")
	}
}

func Register(cfg *config.Common, logger *zap.Logger) {
	var (
		listenIP      string
		prevIP        string
		registerFirst bool
		sd            SD
		err           error
		load          float64
		w             int64
	)

	if cfg.SD != "" {
		if strings.HasPrefix(cfg.Listen, ":") {
			registerFirst = true
			listenIP = utils.GetLocalIP()
			prevIP = listenIP
		}

		hostname, _ = os.Hostname()
		hostname, _, _ = strings.Cut(hostname, ".")

		sd, err = New(cfg, hostname, logger)
		if err != nil {
			panic("serive discovery type not registered")
		}

		load, err = load_avg.Normalized()
		if err == nil {
			load_avg.Store(load)
		}

		logger.Info("init sd",
			zap.String("hostname", hostname),
		)

		w = load_avg.Weight(cfg.BaseWeight, cfg.DegragedMultiply, cfg.DegragedLoad, load)
		sd.Update(listenIP, cfg.Listen, cfg.SDDc, w)
		sd.Clear(listenIP, cfg.Listen)
	}
LOOP:
	for {
		load, err = load_avg.Normalized()
		if err == nil {
			load_avg.Store(load)
		}
		if sd != nil {
			w = load_avg.Weight(cfg.BaseWeight, cfg.DegragedMultiply, cfg.DegragedLoad, load)

			if registerFirst {
				// if listen on all ip, try to register with first ip
				listenIP = utils.GetLocalIP()
			}

			sd.Update(listenIP, cfg.Listen, cfg.SDDc, w)

			if prevIP != listenIP {
				sd.Delete(prevIP, cfg.Listen, cfg.SDDc)
				prevIP = listenIP
			}
		}
		t := time.After(delay)
		select {
		case <-t:
			continue
		case <-stop:
			break LOOP
		}
	}

	if sd != nil {
		if err := sd.Clear("", ""); err == nil {
			logger.Info("cleanup sd",
				zap.String("hostname", hostname),
			)
		} else {
			logger.Warn("cleanup sd",
				zap.String("hostname", hostname),
				zap.Error(err),
			)
		}
	}
}

func Stop() {
	stop <- struct{}{}
}

func Cleanup(cfg *config.Common, sd SD, checkOnly bool) error {
	if cfg.SD != "" && cfg.SDExpire > 0 {
		ts := time.Now().Unix() - int64(cfg.SDExpire.Seconds())

		if nodes, err := sd.Nodes(); err == nil {
			for _, node := range nodes {
				if node.Flags > 0 {
					if ts > node.Flags {
						if checkOnly {
							fmt.Printf("%s: %s (%s), expired\n", node.Key, node.Value, time.Unix(node.Flags, 0).UTC().Format(time.RFC3339Nano))
						} else {
							if err = sd.DeleteNode(node.Key); err != nil {
								return err
							}

							fmt.Printf("%s: %s (%s), deleted\n", node.Key, node.Value, time.Unix(node.Flags, 0).UTC().Format(time.RFC3339Nano))
						}
					}
				} else {
					fmt.Printf("%s: %s (%s)\n", node.Key, node.Value, time.Unix(node.Flags, 0).UTC().Format(time.RFC3339Nano))
				}
			}
		} else {
			return err
		}
	}

	return nil
}
