//go:build test_sd
// +build test_sd

package nginx

import (
	"sort"
	"testing"

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

func TestNginx(t *testing.T) {
	logger := zapwriter.Default()

	sd1 := New("http://127.0.0.1:8500/v1/kv/upstreams", "graphite", hostname1, logger)
	sd2 := New("http://127.0.0.1:8500/v1/kv/upstreams", "graphite", hostname2, logger)

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

	// register new
	require.NoError(t, sd1.Update(ip1, port, nil, 10))
	nodes, err = sd1.List()
	require.NoError(t, err)
	sort.Strings(nodes)
	assert.Equal(
		t, []string{
			"_/test_host1/192.168.0.1:9090",
		}, nodes,
	)
	nodesMap, err := sd1.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"_/test_host1/192.168.0.1:9090": `{"weight":10,"max_fails":0}`,
		}, nodesMap,
	)

	// register new
	require.NoError(t, sd2.Update(ip2, port, nil, 21))
	nodes, err = sd2.List()
	sort.Strings(nodes)
	require.NoError(t, err)
	assert.Equal(
		t, []string{
			"_/test_host2/192.168.1.25:9090",
		},
		nodes,
	)
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"_/test_host2/192.168.1.25:9090": `{"weight":21,"max_fails":0}`,
		}, nodesMap,
	)

	// update
	require.NoError(t, sd2.Update(ip2, port, nil, 25))
	nodes, err = sd2.List()
	sort.Strings(nodes)
	require.NoError(t, err)
	assert.Equal(
		t, []string{
			"_/test_host2/192.168.1.25:9090",
		},
		nodes,
	)
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"_/test_host2/192.168.1.25:9090": `{"weight":25,"max_fails":0}`,
		}, nodesMap,
	)

	// delete
	require.NoError(t, sd2.Delete(ip2, port, nil))
	nodes, err = sd2.List()
	sort.Strings(nodes)
	require.NoError(t, err)
	assert.Equal(t, []string{}, nodes)

	nodesMap, err = sd1.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"_/test_host1/192.168.0.1:9090": `{"weight":10,"max_fails":0}`,
		}, nodesMap,
	)

	// cleanup
	require.NoError(t, sd2.Update(ip2, port, nil, 25))
	require.NoError(t, sd2.Update(ip1, port, nil, 25))
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"_/test_host2/192.168.1.25:9090": `{"weight":25,"max_fails":0}`,
			"_/test_host2/192.168.0.1:9090":  `{"weight":25,"max_fails":0}`,
		}, nodesMap,
	)
	require.NoError(t, sd2.Clear(ip2, port))
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"_/test_host2/192.168.1.25:9090": `{"weight":25,"max_fails":0}`,
		}, nodesMap,
	)

	// clear all
	require.NoError(t, sd1.Clear("", ""))
	nodes, err = sd1.List()
	require.NoError(t, err)
	assert.Equal(t, []string{}, nodes)

	require.NoError(t, sd2.Clear("", ""))
	nodes, err = sd2.List()
	require.True(t, err == nil || err == utils.ErrNotFound, err)
	assert.Equal(t, nilStringSlice, nodes)
}

func TestNginxDC(t *testing.T) {
	logger := zapwriter.Default()

	sd1 := New("http://127.0.0.1:8500/v1/kv/upstreams", "graphite", hostname1, logger)
	sd2 := New("http://127.0.0.1:8500/v1/kv/upstreams", "graphite", hostname2, logger)

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

	// register new
	require.NoError(t, sd1.Update(ip1, port, dc1, 10))
	nodes, err = sd1.List()
	require.NoError(t, err)
	sort.Strings(nodes)
	assert.Equal(
		t, []string{
			"dc1/test_host1/192.168.0.1:9090",
			"dc2/test_host1/192.168.0.1:9090",
			"dc3/test_host1/192.168.0.1:9090",
		}, nodes,
	)
	nodesMap, err := sd1.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"dc1/test_host1/192.168.0.1:9090": `{"weight":10,"max_fails":0}`,
			"dc2/test_host1/192.168.0.1:9090": `{"backup":1, "max_fails":0}`,
			"dc3/test_host1/192.168.0.1:9090": `{"backup":1, "max_fails":0}`,
		}, nodesMap,
	)

	// register new
	require.NoError(t, sd2.Update(ip2, port, dc2, 21))
	nodes, err = sd2.List()
	sort.Strings(nodes)
	require.NoError(t, err)
	assert.Equal(
		t, []string{
			"dc1/test_host2/192.168.1.25:9090",
			"dc2/test_host2/192.168.1.25:9090",
			"dc3/test_host2/192.168.1.25:9090",
		},
		nodes,
	)
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"dc2/test_host2/192.168.1.25:9090": `{"weight":21,"max_fails":0}`,
			"dc1/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
			"dc3/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
		}, nodesMap,
	)

	// update
	require.NoError(t, sd2.Update(ip2, port, dc2, 25))
	nodes, err = sd2.List()
	sort.Strings(nodes)
	require.NoError(t, err)
	assert.Equal(
		t, []string{
			"dc1/test_host2/192.168.1.25:9090",
			"dc2/test_host2/192.168.1.25:9090",
			"dc3/test_host2/192.168.1.25:9090",
		},
		nodes,
	)
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"dc2/test_host2/192.168.1.25:9090": `{"weight":25,"max_fails":0}`,
			"dc1/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
			"dc3/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
		}, nodesMap,
	)

	// delete
	require.NoError(t, sd2.Delete(ip2, port, dc2))
	nodes, err = sd2.List()
	sort.Strings(nodes)
	require.NoError(t, err)
	assert.Equal(t, []string{}, nodes)

	nodesMap, err = sd1.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"dc1/test_host1/192.168.0.1:9090": `{"weight":10,"max_fails":0}`,
			"dc2/test_host1/192.168.0.1:9090": `{"backup":1, "max_fails":0}`,
			"dc3/test_host1/192.168.0.1:9090": `{"backup":1, "max_fails":0}`,
		}, nodesMap,
	)

	// cleanup
	require.NoError(t, sd2.Update(ip2, port, dc2, 25))
	require.NoError(t, sd2.Update(ip1, port, dc2, 25))
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"dc2/test_host2/192.168.1.25:9090": `{"weight":25,"max_fails":0}`,
			"dc1/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
			"dc3/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
			"dc2/test_host2/192.168.0.1:9090":  `{"weight":25,"max_fails":0}`,
			"dc1/test_host2/192.168.0.1:9090":  `{"backup":1, "max_fails":0}`,
			"dc3/test_host2/192.168.0.1:9090":  `{"backup":1, "max_fails":0}`,
		}, nodesMap,
	)
	require.NoError(t, sd2.Clear(ip2, port))
	nodesMap, err = sd2.ListMap()
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"dc2/test_host2/192.168.1.25:9090": `{"weight":25,"max_fails":0}`,
			"dc1/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
			"dc3/test_host2/192.168.1.25:9090": `{"backup":1, "max_fails":0}`,
		}, nodesMap,
	)

	// clear all
	require.NoError(t, sd1.Clear("", ""))
	nodes, err = sd1.List()
	require.NoError(t, err)
	assert.Equal(t, []string{}, nodes)

	require.NoError(t, sd2.Clear("", ""))
	nodes, err = sd2.List()
	assert.Equal(t, nilStringSlice, nodes)
	assert.Equal(t, nilStringSlice, nodes)
}
