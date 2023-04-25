package nginx

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/sd/utils"

	jsoniter "github.com/json-iterator/go"
	"github.com/msaf1980/go-stringutils"
	"go.uber.org/zap"
)

var (
	json          = jsoniter.ConfigCompatibleWithStandardLibrary
	ErrNoKey      = errors.New("list key no found")
	ErrInvalidKey = errors.New("list key is invalid")

	timeNow = time.Now
)

func splitNode(node string) (dc, host, listen string, ok bool) {
	var v string
	dc, v, ok = strings.Cut(node, "/")
	if !ok {
		return
	}
	host, v, ok = strings.Cut(v, "/")
	if !ok {
		return
	}
	listen, _, ok = strings.Cut(v, "/")
	ok = !ok
	return
}

// Nginx register node for https://github.com/weibocom/nginx-upsync-module (with consul)
type Nginx struct {
	weight     int64
	hostname   string
	body       []byte
	backupBody []byte
	url        stringutils.Builder
	pos        int // truncate offset for base address
	nsEnd      string

	logger *zap.Logger
}

func New(url, namespace, hostname string, logger *zap.Logger) *Nginx {
	sd := &Nginx{
		logger:     logger,
		body:       make([]byte, 128),
		backupBody: []byte(`{"backup":1,"max_fails":0}`),
		nsEnd:      "upstreams/" + namespace + "/",
		hostname:   hostname,
	}
	sd.setWeight(0)

	sd.url.WriteString(url)
	sd.url.WriteByte('/')
	if namespace != "" {
		sd.url.WriteString(namespace)
		sd.url.WriteByte('/')
	}
	sd.pos = sd.url.Len()

	return sd
}

func (sd *Nginx) setWeight(weight int64) {
	if sd.weight != weight {
		sd.weight = weight
		sd.body = sd.body[:0]
		sd.body = append(sd.body, `{"weight":`...)
		sd.body = strconv.AppendInt(sd.body, weight, 10)
		sd.body = append(sd.body, `,"max_fails":0}`...)
	}
}

func (sd *Nginx) List() (nodes []string, err error) {
	sd.url.Truncate(sd.pos)
	sd.url.WriteString("?recurse")
	var data []byte
	data, err = utils.HttpGet(sd.url.String())
	if err != nil {
		return
	}
	var iNodes []interface{}
	if err = json.Unmarshal(data, &iNodes); err != nil {
		return nil, err
	}
	nodes = make([]string, 0, len(iNodes))

	for _, i := range iNodes {
		if jNode, ok := i.(map[string]interface{}); ok {
			if i, ok := jNode["Key"]; ok {
				if s, ok := i.(string); ok {
					if strings.HasPrefix(s, sd.nsEnd) {
						s = s[len(sd.nsEnd):]
						_, host, _, ok := splitNode(s)
						if ok && host == sd.hostname {
							nodes = append(nodes, s)
						}
					} else {
						return nil, ErrInvalidKey
					}
				} else {
					return nil, ErrNoKey
				}
			}
		} else {
			return nil, ErrNoKey
		}
	}

	return
}

func (sd *Nginx) ListMap() (nodes map[string]string, err error) {
	sd.url.Truncate(sd.pos)
	sd.url.WriteString("?recurse")
	var data []byte
	data, err = utils.HttpGet(sd.url.String())
	if err != nil {
		return
	}
	var iNodes []interface{}
	if err = json.Unmarshal(data, &iNodes); err != nil {
		return nil, err
	}
	nodes = make(map[string]string)

	for _, i := range iNodes {
		if jNode, ok := i.(map[string]interface{}); ok {
			if i, ok := jNode["Key"]; ok {
				if s, ok := i.(string); ok {
					if strings.HasPrefix(s, sd.nsEnd) {
						s = s[len(sd.nsEnd):]
						_, host, _, ok := splitNode(s)
						if ok && host == sd.hostname {
							if i, ok := jNode["Value"]; ok {
								if v, ok := i.(string); ok {
									d, err := base64.StdEncoding.DecodeString(v)
									if err != nil {
										return nil, err
									}
									nodes[s] = stringutils.UnsafeString(d)
								} else {
									nodes[s] = ""
								}
							} else {
								nodes[s] = ""
							}
						}
					} else {
						return nil, ErrInvalidKey
					}
				} else {
					return nil, ErrNoKey
				}
			}
		} else {
			return nil, ErrNoKey
		}
	}

	return
}

func (sd *Nginx) Nodes() (nodes []utils.KV, err error) {
	sd.url.Truncate(sd.pos)
	sd.url.WriteString("?recurse")
	var data []byte
	data, err = utils.HttpGet(sd.url.String())
	if err != nil {
		return
	}
	var iNodes []interface{}
	if err = json.Unmarshal(data, &iNodes); err != nil {
		return nil, err
	}
	nodes = make([]utils.KV, 0, 3)

	for _, i := range iNodes {
		if jNode, ok := i.(map[string]interface{}); ok {
			if i, ok := jNode["Key"]; ok {
				if s, ok := i.(string); ok {
					if strings.HasPrefix(s, sd.nsEnd) {
						s = s[len(sd.nsEnd):]
						kv := utils.KV{Key: s}
						if i, ok := jNode["Value"]; ok {
							if v, ok := i.(string); ok {
								d, err := base64.StdEncoding.DecodeString(v)
								if err != nil {
									return nil, err
								}
								kv.Value = stringutils.UnsafeString(d)
							}
						}
						if i, ok := jNode["Flags"]; ok {
							switch v := i.(type) {
							case float64:
								kv.Flags = int64(v)
							case int:
								kv.Flags = int64(v)
							case int64:
								kv.Flags = v
							}
						}
						nodes = append(nodes, kv)
					} else {
						return nil, ErrInvalidKey
					}
				} else {
					return nil, ErrNoKey
				}
			}
		} else {
			return nil, ErrNoKey
		}
	}

	return
}

func (sd *Nginx) update(ip, port string, dc []string) (err error) {
	if len(dc) == 0 {
		sd.url.Truncate(sd.pos)
		sd.url.WriteString("_/")
		sd.url.WriteString(sd.hostname)
		sd.url.WriteByte('/')
		if ip != "" {
			sd.url.WriteString(ip)
		}
		sd.url.WriteString(port)

		// add custom query flags
		sd.url.WriteByte('?')
		sd.url.WriteString("flags=")
		sd.url.WriteInt(timeNow().Unix(), 10)

		if err = utils.HttpPut(sd.url.String(), sd.body); err != nil {
			sd.logger.Error("put", zap.String("address", sd.url.String()[sd.pos:]), zap.Error(err))
			return
		}
	} else {
		flags := make([]byte, 0, 32)
		flags = append(flags, "?flags="...)
		flags = strconv.AppendInt(flags, timeNow().Unix(), 10)

		for i := 0; i < len(dc); i++ {
			// cfg.Common.SDDc
			sd.url.Truncate(sd.pos)
			sd.url.WriteString(dc[i])
			sd.url.WriteByte('/')
			sd.url.WriteString(sd.hostname)
			sd.url.WriteByte('/')
			n := sd.url.Len()
			if ip != "" {
				sd.url.WriteString(ip)
			}
			sd.url.WriteString(port)

			// add custom query flags
			sd.url.Write(flags)

			if i == 0 {
				if nErr := utils.HttpPut(sd.url.String(), sd.body); nErr != nil {
					sd.logger.Error(
						"put", zap.String("address", sd.url.String()[n:]), zap.String("dc", dc[i]), zap.Error(nErr),
					)
					err = nErr
				}
			} else {
				if nErr := utils.HttpPut(sd.url.String(), sd.backupBody); nErr != nil {
					sd.logger.Error(
						"put", zap.String("address", sd.url.String()[n:]), zap.String("dc", dc[i]), zap.Error(nErr),
					)
					err = nErr
				}
			}
		}
	}

	return
}

func (sd *Nginx) Update(ip, port string, dc []string, weight int64) error {
	sd.setWeight(weight)

	return sd.update(ip, port, dc)
}

func (sd *Nginx) Delete(ip, port string, dc []string) (err error) {
	if len(dc) == 0 {
		sd.url.Truncate(sd.pos)
		sd.url.WriteString("_/")
		sd.url.WriteString(sd.hostname)
		sd.url.WriteByte('/')
		if ip != "" {
			sd.url.WriteString(ip)
		}
		sd.url.WriteString(port)

		if err = utils.HttpDelete(sd.url.String()); err != nil {
			sd.logger.Error("delete", zap.String("address", sd.url.String()[sd.pos:]), zap.Error(err))
		}
	} else {
		for i := 0; i < len(dc); i++ {
			// cfg.Common.SDDc
			sd.url.Truncate(sd.pos)
			sd.url.WriteString(dc[i])
			sd.url.WriteByte('/')
			sd.url.WriteString(sd.hostname)
			sd.url.WriteByte('/')
			n := sd.url.Len()
			if ip != "" {
				sd.url.WriteString(ip)
			}
			sd.url.WriteString(port)

			if nErr := utils.HttpDelete(sd.url.String()); nErr != nil {
				sd.logger.Error(
					"delete", zap.String("address", sd.url.String()[n:]), zap.String("dc", dc[i]), zap.Error(nErr),
				)
				err = nErr
			}
		}
	}

	return
}

func (sd *Nginx) Clear(preserveIP, preservePort string) (err error) {
	var nodes []string
	nodes, err = sd.List()
	if err != nil {
		sd.logger.Error(
			"list", zap.String("address", sd.url.String()[sd.pos:]), zap.Error(err),
		)
		return
	}
	if len(nodes) == 0 {
		return
	}
	preserveListen := preserveIP + preservePort
	sd.url.WriteByte('/')
	for _, node := range nodes {
		sd.url.Truncate(sd.pos)
		_, host, listen, _ := splitNode(node)
		if host == sd.hostname && listen != preserveListen {
			sd.url.WriteString(node)
			if nErr := utils.HttpDelete(sd.url.String()); nErr != nil {
				sd.logger.Error(
					"delete", zap.String("address", sd.url.String()), zap.Error(nErr),
				)
				err = nErr
			}
		}
	}

	return
}
