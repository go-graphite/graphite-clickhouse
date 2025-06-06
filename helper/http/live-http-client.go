package http

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

const TCPNetwork string = "tcp"

func DoHTTPOverTCP(ctx context.Context, transport *http.Transport, req *http.Request) (*http.Response, error) {
	conn, err := transport.DialContext(context.Background(), TCPNetwork, req.URL.Host)
	if err != nil {
		return nil, err
	}

	err = req.Write(conn)
	if err != nil {
		return nil, err
	}

	var backup_buf bytes.Buffer
	reader := bufio.NewReader(io.TeeReader(conn, &backup_buf))

	HEADERS:
	for {
		select {
		case <- ctx.Done():
			fake_body_delimer := bytes.NewBuffer([]byte{'\r', '\n', '\r', '\n'})
			resp, err := http.ReadResponse(bufio.NewReader(io.MultiReader(&backup_buf, fake_body_delimer)), nil)
			if err != nil {
				return nil, err
			}
			return resp, io.EOF
		
		default:
			line, err := reader.ReadString('\n')
			fmt.Println(line)
			if err != nil {
				return nil, err
			}
			// if strings.TrimSpace(line) == "" || line == "\r\n" {
			if line == "\r\n" {
				break HEADERS
			}
		}
	}

	full_resp_stream := io.MultiReader(&backup_buf, conn)
	resp, err := http.ReadResponse(bufio.NewReader(full_resp_stream), nil)
	return resp, err
}

