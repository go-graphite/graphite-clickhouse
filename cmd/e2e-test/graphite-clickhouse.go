package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"syscall"
	"text/template"
)

type GraphiteClickhouse struct {
	Binary    string `toml:"binary"`
	ConfigTpl string `toml:"template"`

	storeDir   string    `toml:"-"`
	configFile string    `toml:"-"`
	address    string    `toml:"-"`
	cmd        *exec.Cmd `toml:"-"`
}

func (c *GraphiteClickhouse) Start(testDir, clickhouseAddr string) error {
	if c.cmd != nil {
		return fmt.Errorf("carbon-clickhouse already started")
	}

	if len(c.Binary) == 0 {
		c.Binary = "./graphite-clickhouse"
	}
	if len(c.ConfigTpl) == 0 {
		return fmt.Errorf("graphite-clickhouse config template not set")
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
		CLICKHOUSE_ADDR string
		GCH_ADDR        string
		GCH_DIR         string
	}{
		CLICKHOUSE_ADDR: clickhouseAddr,
		GCH_ADDR:        c.address,
		GCH_DIR:         c.storeDir,
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
	//c.cmd.Env = append(c.cmd.Env, "TZ=UTC")
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
	req, err := http.DefaultClient.Get("http://" + c.address + "/alive")
	if err == nil || req.StatusCode != http.StatusOK {
		return false
	}
	return true
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

func (c *GraphiteClickhouse) Address() string {
	return c.address
}
