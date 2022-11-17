package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/msaf1980/go-stringutils"
)

var ClickhouseContainerName = "clickhouse-server-gch-test"

type Clickhouse struct {
	Version string `toml:"version"`
	Dir     string `toml:"dir"`

	DockerImage string `toml:"image"`

	TZ string `toml:"tz"` // override timezone

	httpAddress string `toml:"-"`
	url         string `toml:"-"`
	container   string `toml:"-"`
}

func (c *Clickhouse) Start() (string, error) {
	if len(c.Version) == 0 {
		return "", errors.New("version not set")
	}
	if len(c.Dir) == 0 {
		return "", errors.New("dir not set")
	}
	if len(c.DockerImage) == 0 {
		c.DockerImage = "yandex/clickhouse-server"
	}
	var err error
	c.httpAddress, err = getFreeTCPPort("")
	if err != nil {
		return "", err
	}
	c.url = "http://" + c.httpAddress

	c.container = ClickhouseContainerName

	// tz, _ := localTZLocationName()

	chStart := []string{"run", "-d",
		"--name", c.container,
		"--ulimit", "nofile=262144:262144",
		"-p", c.httpAddress + ":8123",
		// "-e", "TZ=" + tz, // workaround for TZ=":/etc/localtime"
		"-v", c.Dir + "/config.xml:/etc/clickhouse-server/config.xml",
		"-v", c.Dir + "/users.xml:/etc/clickhouse-server/users.xml",
		"-v", c.Dir + "/rollup.xml:/etc/clickhouse-server/config.d/rollup.xml",
		"-v", c.Dir + "/init.sql:/docker-entrypoint-initdb.d/init.sql",
	}
	if c.TZ != "" {
		chStart = append(chStart, "-e", "TZ="+c.TZ)
	}

	chStart = append(chStart, c.DockerImage+":"+c.Version)

	cmd := exec.Command(DockerBinary, chStart...)
	out, err := cmd.CombinedOutput()

	return string(out), err
}

func (c *Clickhouse) Stop(delete bool) (string, error) {
	if len(c.container) == 0 {
		return "", nil
	}

	chStop := []string{"stop", c.container}

	cmd := exec.Command(DockerBinary, chStop...)
	out, err := cmd.CombinedOutput()

	if err == nil && delete {
		return c.Delete()
	}
	return string(out), err
}

func (c *Clickhouse) Delete() (string, error) {
	if len(c.container) == 0 {
		return "", nil
	}

	chDel := []string{"rm", c.container}

	cmd := exec.Command(DockerBinary, chDel...)
	out, err := cmd.CombinedOutput()

	if err == nil {
		c.container = ""
	}

	return string(out), err
}

func (c *Clickhouse) URL() string {
	return c.url
}

func (c *Clickhouse) Container() string {
	return c.container
}

func (c *Clickhouse) Exec(sql string) (bool, string) {
	return containerExec(c.container, []string{"sh", "-c", "clickhouse-client -q '" + sql + "'"})
}

func (c *Clickhouse) Query(sql string) (string, error) {
	reader := strings.NewReader(sql)
	request, err := http.NewRequest("POST", c.URL(), reader)
	if err != nil {
		return "", err
	}

	httpClient := http.Client{
		Timeout: time.Minute,
	}
	resp, err := httpClient.Do(request)
	if err != nil {
		return "", err
	}
	msg, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status + ": " + string(bytes.TrimRight(msg, "\n")))
	}
	return string(msg), nil
}

func (c *Clickhouse) Alive() bool {
	if len(c.container) == 0 {
		return false
	}
	req, err := http.DefaultClient.Get(c.url)
	if err != nil {
		return false
	}
	return req.StatusCode == http.StatusOK
}

func (c *Clickhouse) Logs() {
	if len(c.container) == 0 {
		return
	}

	chArgs := []string{"logs", c.container}

	cmd := exec.Command(DockerBinary, chArgs...)
	out, _ := cmd.CombinedOutput()
	fmt.Fprintln(os.Stderr, stringutils.UnsafeString(out))
}
