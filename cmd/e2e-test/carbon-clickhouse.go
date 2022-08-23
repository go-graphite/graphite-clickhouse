package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"text/template"
)

type CarbonClickhouse struct {
	Version string `toml:"version"`

	Docker      string `toml:"docker"`
	DockerImage string `toml:"image"`

	Template string `toml:"template"` // carbon-clickhouse config template

	address   string `toml:"-"`
	container string `toml:"-"`
	storeDir  string `toml:"-"`
}

func (c *CarbonClickhouse) Start(testDir, clickhouseAddr string, clickhouseContainer string) (error, string) {
	if len(c.Version) == 0 {
		return fmt.Errorf("version not set"), ""
	}
	if len(c.Docker) == 0 {
		c.Docker = "docker"
	}
	if len(c.DockerImage) == 0 {
		c.DockerImage = "lomik/carbon-clickhouse"
	}
	var err error
	c.address, err = getFreeTCPPort("")
	if err != nil {
		return err, ""
	}

	c.container = "carbon-clickhouse-gch-test"

	c.storeDir, err = ioutil.TempDir("", "carbon-clickhouse")
	if err != nil {
		return err, ""
	}

	c.address, err = getFreeTCPPort("")
	if err != nil {
		c.Cleanup()
		return err, ""
	}

	name := filepath.Base(c.Template)
	tpl := path.Join(testDir, c.Template)
	tmpl, err := template.New(name).ParseFiles(tpl)
	if err != nil {
		c.Cleanup()
		return err, ""
	}
	param := struct {
		CLICKHOUSE_ADDR string
		CCH_ADDR        string
	}{
		CLICKHOUSE_ADDR: clickhouseAddr,
		CCH_ADDR:        c.address,
	}

	configFile := path.Join(c.storeDir, "carbon-clickhouse.conf")
	f, err := os.OpenFile(configFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		c.Cleanup()
		return err, ""
	}
	err = tmpl.ExecuteTemplate(f, name, param)
	if err != nil {
		c.Cleanup()
		return err, ""
	}

	cchStart := []string{"run", "-d",
		"--name", c.container,
		"-p", c.address + ":2003",
		//"-e", "TZ=UTC",
		"-v", c.storeDir + ":/etc/carbon-clickhouse",
		"--link", clickhouseContainer,
		c.DockerImage + ":" + c.Version,
	}

	cmd := exec.Command(c.Docker, cchStart...)
	out, err := cmd.CombinedOutput()

	return err, string(out)
}

func (c *CarbonClickhouse) Stop(delete bool) (error, string) {
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

func (c *CarbonClickhouse) Delete() (error, string) {
	if len(c.container) == 0 {
		return nil, ""
	}

	chDel := []string{"rm", c.container}

	cmd := exec.Command(c.Docker, chDel...)
	out, err := cmd.CombinedOutput()

	if err == nil {
		c.container = ""
	}

	c.Cleanup()

	return err, string(out)
}

func (c *CarbonClickhouse) Cleanup() {
	if c.storeDir != "" {
		os.RemoveAll(c.storeDir)
		c.storeDir = ""
	}
}

func (c *CarbonClickhouse) Address() string {
	return c.address
}

func (c *CarbonClickhouse) Container() string {
	return c.container
}
