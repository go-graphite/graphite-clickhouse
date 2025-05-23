package client

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"

	pickle "github.com/lomik/og-rek"
)

type FindMatch struct {
	Path   string `toml:"path"`
	IsLeaf bool   `toml:"is_leaf"`
}

// MetricsFind do /metrics/find/ request
// Valid formats are carbonapi_v3_pb. protobuf, pickle
func MetricsFind(client *http.Client, address string, format FormatType, query string, from, until int64) (string, []FindMatch, http.Header, error) {
	if format == FormatDefault {
		format = FormatPb_v3
	}

	rUrl := "/metrics/find/"

	queryParams := fmt.Sprintf("%s?format=%s, from=%d, until=%d, query %s", rUrl, format.String(), from, until, query)

	var fromStr, untilStr string

	u, err := url.Parse(address + rUrl)
	if err != nil {
		return queryParams, nil, nil, err
	}

	v := url.Values{
		"format": []string{format.String()},
	}

	var reader io.Reader

	switch format {
	case FormatPb_v3:
		var body []byte

		r := protov3.MultiGlobRequest{
			Metrics:   []string{query},
			StartTime: from,
			StopTime:  until,
		}

		body, err = r.Marshal()
		if err != nil {
			return query, nil, nil, err
		}

		if body != nil {
			reader = bytes.NewReader(body)
		}
	case FormatProtobuf, FormatPickle:
		v["query"] = []string{query}
		if from > 0 {
			v["from"] = []string{fromStr}
		}

		if until > 0 {
			v["until"] = []string{untilStr}
		}
	default:
		return queryParams, nil, nil, ErrUnsupportedFormat
	}

	u.RawQuery = v.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), reader)
	if err != nil {
		return queryParams, nil, nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return queryParams, nil, nil, err
	}

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return queryParams, nil, nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return queryParams, nil, resp.Header, nil
	} else if resp.StatusCode != http.StatusOK {
		return queryParams, nil, resp.Header, NewHttpError(resp.StatusCode, string(b))
	}

	var globs []FindMatch

	switch format {
	case FormatProtobuf:
		var globsv2 protov2.GlobResponse
		if err = globsv2.Unmarshal(b); err != nil {
			return queryParams, nil, resp.Header, err
		}

		for _, m := range globsv2.Matches {
			globs = append(globs, FindMatch{Path: m.Path, IsLeaf: m.IsLeaf})
		}
	case FormatPb_v3:
		var globsv3 protov3.MultiGlobResponse
		if err = globsv3.Unmarshal(b); err != nil {
			return queryParams, nil, resp.Header, err
		}

		for _, m := range globsv3.Metrics {
			for _, v := range m.Matches {
				globs = append(globs, FindMatch{Path: v.Path, IsLeaf: v.IsLeaf})
			}
		}
	case FormatPickle:
		reader := bytes.NewReader(b)
		decoder := pickle.NewDecoder(reader)

		p, err := decoder.Decode()
		if err != nil {
			return queryParams, nil, resp.Header, err
		}

		for _, v := range p.([]interface{}) {
			m := v.(map[interface{}]interface{})
			path := m["metric_path"].(string)
			isLeaf := m["isLeaf"].(bool)
			globs = append(globs, FindMatch{Path: path, IsLeaf: isLeaf})
		}
	default:
		return queryParams, nil, resp.Header, ErrUnsupportedFormat
	}

	return queryParams, globs, resp.Header, nil
}
