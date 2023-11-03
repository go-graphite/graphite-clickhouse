//go:build test_sd
// +build test_sd

package nginx_test

import (
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/sd"
	"github.com/lomik/graphite-clickhouse/sd/utils"
	"github.com/lomik/zapwriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	hostname1 = "test_host1"
	ip1       = "192.168.0.1"
	hostname2 = "test_host2"
	ip2       = "192.168.1.25"
	port      = ":9090"
	dc1       = []string{"dc1", "dc2", "dc3"}
	dc2       = []string{"dc2", "dc1", "dc3"}
	hostname3 = "test_host3"

	nilStringSlice []string
)

func cleanup(nodes []utils.KV, start, end int64) {
	for i := range nodes {
		if nodes[i].Flags >= start && nodes[i].Flags <= end {
			nodes[i].Flags = start
		}
	}
}

func TestNginxExpire(t *testing.T) {
	logger := zapwriter.Default()

	cfg := &config.Common{
		SDType:      config.SDNginx,
		SD:          "http://127.0.0.1:8500/v1/kv/upstreams",
		SDNamespace: "graphite", //default
		SDExpire:    time.Second * 5,
	}

	sd1, _ := sd.New(cfg, hostname1, logger)
	sd2, _ := sd.New(cfg, hostname2, logger)

	err := sd1.Clear("", "")
	require.True(t, err == nil || err == utils.ErrNotFound, err)
	err = sd2.Clear("", "")
	require.True(t, err == nil || err == utils.ErrNotFound, err)

	nodes, err := sd1.List()
	require.True(t, err == nil || err == utils.ErrNotFound, err)
	assert.Equal(t, nilStringSlice, nodes)
	nodes, err = sd2.List()
	require.True(t, err == nil || err == utils.ErrNotFound, err)
	assert.Equal(t, nilStringSlice, nodes)

	// check cleanup expired
	start := time.Now().Unix()
	require.NoError(t, sd1.Update(ip1, port, nil, 10))
	time.Sleep(cfg.SDExpire + time.Second)
	require.NoError(t, sd2.Update(ip2, port, nil, 10))
	nodesV, err := sd1.Nodes()
	end := time.Now().Unix()
	require.NoError(t, err)
	// reset timestamp for compare
	cleanup(nodesV, start, end)
	assert.Equal(
		t,
		[]utils.KV{
			{Key: "_/test_host1/192.168.0.1:9090", Value: "{\"weight\":10,\"max_fails\":0}", Flags: start},
			{Key: "_/test_host2/192.168.1.25:9090", Value: "{\"weight\":10,\"max_fails\":0}", Flags: start},
		},
		nodesV,
		"start = %d, end = %d", start, end,
	)

	sd.Cleanup(cfg, sd1, false)
	nodesV, err = sd1.Nodes()
	require.NoError(t, err)
	// reset timestamp for compare
	cleanup(nodesV, start, end)
	assert.Equal(
		t,
		[]utils.KV{
			{Key: "_/test_host2/192.168.1.25:9090", Value: "{\"weight\":10,\"max_fails\":0}", Flags: start},
		},
		nodesV,
		"start = %d, end = %d", start, end,
	)
}

func TestNginxExpireDC(t *testing.T) {
	logger := zapwriter.Default()

	cfg1 := &config.Common{
		SDType:      config.SDNginx,
		SD:          "http://127.0.0.1:8500/v1/kv/upstreams",
		SDNamespace: "graphite", //default
		SDDc:        dc1,
		SDExpire:    time.Second * 5,
	}
	sd1, _ := sd.New(cfg1, hostname1, logger)

	cfg2 := &config.Common{
		SDType:      config.SDNginx,
		SD:          "http://127.0.0.1:8500/v1/kv/upstreams",
		SDNamespace: "", //default
		SDDc:        dc2,
		SDExpire:    time.Second * 5,
	}
	sd2, _ := sd.New(cfg2, hostname2, logger)

	err := sd1.Clear("", "")
	require.True(t, err == nil || err == utils.ErrNotFound, err)
	err = sd2.Clear("", "")
	require.True(t, err == nil || err == utils.ErrNotFound, err)

	nodes, err := sd1.List()
	require.True(t, err == nil || err == utils.ErrNotFound, err)
	assert.Equal(t, nilStringSlice, nodes)
	nodes, err = sd2.List()
	require.True(t, err == nil || err == utils.ErrNotFound, err)
	assert.Equal(t, nilStringSlice, nodes)

	// check cleanup expired
	start := time.Now().Unix()
	require.NoError(t, sd1.Update(ip1, port, dc1, 10))
	time.Sleep(cfg1.SDExpire + time.Second)
	require.NoError(t, sd2.Update(ip2, port, dc2, 10))
	nodesV, err := sd1.Nodes()
	end := time.Now().Unix()
	require.NoError(t, err)
	// reset timestamp for compare
	cleanup(nodesV, start, end)
	assert.Equal(
		t,
		[]utils.KV{
			{Key: "dc1/test_host1/192.168.0.1:9090", Value: "{\"weight\":10,\"max_fails\":0}", Flags: start},
			{Key: "dc1/test_host2/192.168.1.25:9090", Value: "{\"backup\":1,\"max_fails\":0}", Flags: start},
			{Key: "dc2/test_host1/192.168.0.1:9090", Value: "{\"backup\":1,\"max_fails\":0}", Flags: start},
			{Key: "dc2/test_host2/192.168.1.25:9090", Value: "{\"weight\":10,\"max_fails\":0}", Flags: start},
			{Key: "dc3/test_host1/192.168.0.1:9090", Value: "{\"backup\":1,\"max_fails\":0}", Flags: start},
			{Key: "dc3/test_host2/192.168.1.25:9090", Value: "{\"backup\":1,\"max_fails\":0}", Flags: start},
		},
		nodesV,
		"start = %d, end = %d", start, end,
	)

	sd.Cleanup(cfg1, sd1, false)
	nodesV, err = sd1.Nodes()
	require.NoError(t, err)
	// reset timestamp for compare
	cleanup(nodesV, start, end)
	assert.Equal(
		t,
		[]utils.KV{
			{Key: "dc1/test_host2/192.168.1.25:9090", Value: "{\"backup\":1,\"max_fails\":0}", Flags: start},
			{Key: "dc2/test_host2/192.168.1.25:9090", Value: "{\"weight\":10,\"max_fails\":0}", Flags: start},
			{Key: "dc3/test_host2/192.168.1.25:9090", Value: "{\"backup\":1,\"max_fails\":0}", Flags: start},
		},
		nodesV,
		"start = %d, end = %d", start, end,
	)
}
