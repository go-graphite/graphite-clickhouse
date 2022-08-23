package main

import (
	"fmt"
	"os/exec"
)

type Clickhouse struct {
	Version string `toml:"version"`
	Dir     string `toml:"dir"`

	Docker      string `toml:"docker"`
	DockerImage string `toml:"image"`

	httpAddress string `toml:"-"`
	container   string `toml:"-"`
}

func (c *Clickhouse) Start() (error, string) {
	if len(c.Version) == 0 {
		return fmt.Errorf("version not set"), ""
	}
	if len(c.Dir) == 0 {
		return fmt.Errorf("dir not set"), ""
	}
	if len(c.Docker) == 0 {
		c.Docker = "docker"
	}
	if len(c.DockerImage) == 0 {
		c.DockerImage = "yandex/clickhouse-server"
	}
	var err error
	c.httpAddress, err = getFreeTCPPort("")
	if err != nil {
		return err, ""
	}

	c.container = "clickhouse-server-gch-test"

	chStart := []string{"run", "-d",
		"--name", c.container,
		"--ulimit", "nofile=262144:262144",
		"-p", c.httpAddress + ":8123",
		//"-e", "TZ=UTC",
		"-v", c.Dir + "/config.xml:/etc/clickhouse-server/config.xml",
		"-v", c.Dir + "/users.xml:/etc/clickhouse-server/users.xml",
		"-v", c.Dir + "/rollup.xml:/etc/clickhouse-server/config.d/rollup.xml",
		"-v", c.Dir + "/init.sql:/docker-entrypoint-initdb.d/init.sql",
		c.DockerImage + ":" + c.Version,
	}

	cmd := exec.Command(c.Docker, chStart...)
	out, err := cmd.CombinedOutput()

	return err, string(out)
}

func (c *Clickhouse) Stop(delete bool) (error, string) {
	if len(c.container) == 0 {
		return nil, ""
	}

	chStop := []string{"stop", c.container}

	cmd := exec.Command(c.Docker, chStop...)
	out, err := cmd.CombinedOutput()

	if err == nil && delete {
		return c.Delete()
	}
	return err, string(out)
}

func (c *Clickhouse) Delete() (error, string) {
	if len(c.container) == 0 {
		return nil, ""
	}

	chDel := []string{"rm", c.container}

	cmd := exec.Command(c.Docker, chDel...)
	out, err := cmd.CombinedOutput()

	if err == nil {
		c.container = ""
	}

	return err, string(out)
}

func (c *Clickhouse) HttpAddress() string {
	return c.httpAddress
}

func (c *Clickhouse) Container() string {
	return c.container
}
