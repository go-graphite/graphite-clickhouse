package main

import (
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"text/template"
)

var CchContainerName = "carbon-clickhouse-gch-test"

type CarbonClickhouse struct {
	Version string `toml:"version"`

	DockerImage string `toml:"image"`

	Template string `toml:"template"` // carbon-clickhouse config template

	TZ string `toml:"tz"` // override timezone

	address   string `toml:"-"`
	container string `toml:"-"`
	storeDir  string `toml:"-"`
}

func (c *CarbonClickhouse) Start(testDir, clickhouseURL, clickhouseContainer string) (string, error) {
	if len(c.Version) == 0 {
		return "", errors.New("version not set")
	}
	if len(c.DockerImage) == 0 {
		c.DockerImage = "lomik/carbon-clickhouse"
	}
	var err error
	c.address, err = getFreeTCPPort("")
	if err != nil {
		return "", err
	}

	c.container = CchContainerName

	c.storeDir, err = ioutil.TempDir("", "carbon-clickhouse")
	if err != nil {
		return "", err
	}

	c.address, err = getFreeTCPPort("")
	if err != nil {
		c.Cleanup()
		return "", err
	}

	name := filepath.Base(c.Template)
	tpl := path.Join(testDir, c.Template)
	tmpl, err := template.New(name).ParseFiles(tpl)
	if err != nil {
		c.Cleanup()
		return "", err
	}
	param := struct {
		CLICKHOUSE_URL string
		CCH_ADDR       string
	}{
		CLICKHOUSE_URL: clickhouseURL,
		CCH_ADDR:       c.address,
	}

	configFile := path.Join(c.storeDir, "carbon-clickhouse.conf")
	f, err := os.OpenFile(configFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		c.Cleanup()
		return "", err
	}
	err = tmpl.ExecuteTemplate(f, name, param)
	if err != nil {
		c.Cleanup()
		return "", err
	}

	// tz, _ := localTZLocationName()

	cchStart := []string{"run", "-d",
		"--name", c.container,
		"-p", c.address + ":2003",
		"-v", c.storeDir + ":/etc/carbon-clickhouse",
		"--link", clickhouseContainer,
	}
	if c.TZ != "" {
		cchStart = append(cchStart, "-e", "TZ="+c.TZ)
	}

	cchStart = append(cchStart, c.DockerImage+":"+c.Version)

	cmd := exec.Command(DockerBinary, cchStart...)
	out, err := cmd.CombinedOutput()

	return string(out), err
}

func (c *CarbonClickhouse) Stop(delete bool) (string, error) {
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

func (c *CarbonClickhouse) Delete() (string, error) {
	if len(c.container) == 0 {
		return "", nil
	}

	chDel := []string{"rm", c.container}

	cmd := exec.Command(DockerBinary, chDel...)
	out, err := cmd.CombinedOutput()

	if err == nil {
		c.container = ""
	}

	c.Cleanup()

	return string(out), err
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
