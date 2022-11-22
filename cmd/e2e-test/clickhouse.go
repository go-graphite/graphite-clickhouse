package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/msaf1980/go-stringutils"
)

var ClickhouseContainerName = "clickhouse-server-gch-test"
var ClickhouseOldImage = "yandex/clickhouse-server"
var ClickhouseDefaultImage = "clickhouse/clickhouse-server"

type Clickhouse struct {
	Version string `toml:"version"`
	Dir     string `toml:"dir"`

	DockerImage string `toml:"image"`

	TZ string `toml:"tz"` // override timezone

	httpAddress string `toml:"-"`
	url         string `toml:"-"`
	container   string `toml:"-"`
}

func (c *Clickhouse) CheckConfig(rootDir string) error {
	if c.Version == "" {
		c.Version = "latest"
	}
	if len(c.Dir) == 0 {
		return ErrNoSetDir
	}
	if !strings.HasPrefix(c.Dir, "/") {
		c.Dir = rootDir + "/" + c.Dir
	}

	if c.DockerImage == "" {
		if c.Version == "latest" {
			c.DockerImage = ClickhouseDefaultImage
		} else {
			splitV := strings.Split(c.Version, ".")
			majorV, err := strconv.Atoi(splitV[0])
			if err != nil {
				c.DockerImage = ClickhouseDefaultImage
			} else if majorV >= 21 {
				c.DockerImage = ClickhouseDefaultImage
			} else {
				c.DockerImage = ClickhouseOldImage
			}
		}
	}
	return nil
}

func (c *Clickhouse) Key() string {
	return c.DockerImage + ":" + c.Version + " " + c.Dir + " TZ " + c.TZ
}

func (c *Clickhouse) Start() (string, error) {
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

func (c *Clickhouse) CopyLog(destDir string, tail uint64) error {
	if len(c.container) == 0 {
		return nil
	}
	dest := destDir + "/clickhouse-server.log"

	chArgs := []string{"cp", c.container + ":/var/log/clickhouse-server/clickhouse-server.log", dest}

	cmd := exec.Command(DockerBinary, chArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return errors.New(err.Error() + ": " + string(bytes.TrimRight(out, "\n")))
	}

	if tail > 0 {
		out, _ := exec.Command("tail", "-"+strconv.FormatUint(tail, 10), dest).Output()
		fmt.Fprintf(os.Stderr, "CLICKHOUSE-SERVER.LOG %s", stringutils.UnsafeString(out))
	}

	return nil
}

func (c *Clickhouse) CopyErrLog(destDir string, tail uint64) error {
	if len(c.container) == 0 {
		return nil
	}
	dest := destDir + "/clickhouse-server.err.log"

	chArgs := []string{"cp", c.container + ":/var/log/clickhouse-server/clickhouse-server.err.log", dest}

	cmd := exec.Command(DockerBinary, chArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return errors.New(err.Error() + ": " + string(bytes.TrimRight(out, "\n")))
	}

	if tail > 0 {
		out, _ := exec.Command("tail", "-"+strconv.FormatUint(tail, 10), dest).Output()
		fmt.Fprintf(os.Stderr, "CLICKHOUSE-SERVER.ERR %s", stringutils.UnsafeString(out))
	}

	return nil
}
