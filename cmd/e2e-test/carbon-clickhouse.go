package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
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
		c.Version = "0.11.4"
	}
	if len(c.DockerImage) == 0 {
		c.DockerImage = "ghcr.io/go-graphite/carbon-clickhouse"
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

	tz := os.Getenv("TZ")

	cchStart := []string{"run", "-d",
		"--name", c.container,
		"-p", c.address + ":2003",
		"-v", c.storeDir + ":/etc/carbon-clickhouse",
		// TZ, need to be same as graphite-clickhouse for prevent bugs, ike issue #184
		"-v", "/etc/timezone:/etc/timezone:ro",
		"-v", "/etc/localtime:/etc/localtime:ro",
		"-e", "TZ=" + tz,
		"--link", clickhouseContainer,
	}

	cchStart = append(cchStart, c.DockerImage+":"+c.Version)

	cmd := exec.Command(DockerBinary, cchStart...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		dateLocal, _ := exec.Command("date").Output()
		dateLocalStr := strings.TrimRight(string(dateLocal), "\n")
		_, dateOnCCH := containerExec(c.container, []string{"date"})
		fmt.Printf("date local %s, on carbon-clickhouse %s\n", dateLocalStr, dateOnCCH)
	}

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
