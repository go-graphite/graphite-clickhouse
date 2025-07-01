package http

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"time"
)

const TCPNetwork string = "tcp"

func DoHTTPOverTCP(ctx context.Context, transport *http.Transport, req *http.Request, timeout time.Duration) (*http.Response, error) {
	conn, err := transport.DialContext(ctx, TCPNetwork, req.URL.Host)
	if err != nil {
		return nil, err
	}

	err = conn.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		return nil, err
	}

	err = req.Write(conn)
	if err != nil {
		return nil, err
	}

	var backup_buf bytes.Buffer
	reader := bufio.NewReader(io.TeeReader(conn, &backup_buf))

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fake_body_delimer := bytes.NewBuffer([]byte{'\r', '\n', '\r', '\n'})

				resp, err := http.ReadResponse(bufio.NewReader(io.MultiReader(&backup_buf, fake_body_delimer)), nil)
				if err != nil {
					return nil, err
				}

				return resp, netErr
			}

			return nil, err
		}

		if line == "\r\n" {
			break
		}
	}

	full_resp_stream := io.MultiReader(&backup_buf, conn)
	resp, err := http.ReadResponse(bufio.NewReader(full_resp_stream), nil)

	return resp, err
}
