package main

import (
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"text/template"

	"github.com/lomik/graphite-clickhouse/helper/client"
)

type GraphiteClickhouse struct {
	Binary    string `toml:"binary"`
	ConfigTpl string `toml:"template"`

	TZ string `toml:"tz"` // override timezone

	storeDir   string    `toml:"-"`
	configFile string    `toml:"-"`
	address    string    `toml:"-"`
	cmd        *exec.Cmd `toml:"-"`
}

func (c *GraphiteClickhouse) Start(testDir, clickhouseURL, chProxyURL string) error {
	if c.cmd != nil {
		return errors.New("carbon-clickhouse already started")
	}

	if len(c.Binary) == 0 {
		c.Binary = "./graphite-clickhouse"
	}
	if len(c.ConfigTpl) == 0 {
		return errors.New("graphite-clickhouse config template not set")
	}

	var err error
	c.storeDir, err = ioutil.TempDir("", "graphite-clickhouse")
	if err != nil {
		return err
	}

	c.address, err = getFreeTCPPort("")
	if err != nil {
		c.Cleanup()
		return err
	}

	name := filepath.Base(c.ConfigTpl)
	tmpl, err := template.New(name).ParseFiles(path.Join(testDir, c.ConfigTpl))
	if err != nil {
		c.Cleanup()
		return err
	}
	param := struct {
		CLICKHOUSE_URL string
		PROXY_URL      string
		GCH_ADDR       string
		GCH_DIR        string
	}{
		CLICKHOUSE_URL: clickhouseURL,
		PROXY_URL:      chProxyURL,
		GCH_ADDR:       c.address,
		GCH_DIR:        c.storeDir,
	}

	c.configFile = path.Join(c.storeDir, "graphite-clickhouse.conf")
	f, err := os.OpenFile(c.configFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		c.Cleanup()
		return err
	}
	err = tmpl.ExecuteTemplate(f, name, param)
	if err != nil {
		c.Cleanup()
		return err
	}

	c.cmd = exec.Command(c.Binary, "-config", c.configFile)
	c.cmd.Stdout = os.Stdout
	c.cmd.Stderr = os.Stderr
	if c.TZ != "" {
		c.cmd.Env = append(c.cmd.Env, "TZ="+c.TZ)
	}
	err = c.cmd.Start()
	if err != nil {
		c.Cleanup()
		return err
	}

	return nil
}

func (c *GraphiteClickhouse) Alive() bool {
	if c.cmd == nil {
		return false
	}
	_, _, _, err := client.MetricsFind(http.DefaultClient, "http://"+c.address+"/alive", client.FormatDefault, "NonExistentTarget", 0, 0)
	return err == nil
}

func (c *GraphiteClickhouse) Stop(cleanup bool) error {
	if cleanup {
		defer c.Cleanup()
	}

	if c.cmd == nil {
		return nil
	}
	var err error
	if err = c.cmd.Process.Kill(); err == nil {
		if err = c.cmd.Wait(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
					ec := status.ExitStatus()
					if ec == 0 || ec == -1 {
						return nil
					}
				}
			}
		}
	}
	return err
}

func (c *GraphiteClickhouse) Cleanup() {
	if len(c.storeDir) > 0 {
		os.RemoveAll(c.storeDir)
		c.storeDir = ""
		c.cmd = nil
	}
}

func (c *GraphiteClickhouse) URL() string {
	return "http://" + c.address
}

func (c *GraphiteClickhouse) Cmd() string {
	return strings.Join(c.cmd.Args, " ")
}
