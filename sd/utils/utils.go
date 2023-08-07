package utils

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/errs"
)

var (
	ErrNotFound = errors.New("entry not found")
)

type KV struct {
	Key   string
	Value string
}

func HttpGet(url string) ([]byte, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 300 {
		return nil, errs.NewErrorWithCode(string(data), resp.StatusCode)
	}
	return data, err
}

func HttpPut(url string, body []byte) error {
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return errs.NewErrorWithCode(string(data), resp.StatusCode)
	}
	return nil
}

func HttpDelete(url string) error {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return errs.NewErrorWithCode(string(data), resp.StatusCode)
	}
	return nil
}

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
