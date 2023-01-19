package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	pickle "github.com/lomik/og-rek"
)

var ErrInvalidFrom = errors.New("invalid from")
var ErrInvalidUntil = errors.New("invalid until")

type Metric struct {
	Name                    string    `toml:"name"`
	PathExpression          string    `toml:"path"`
	ConsolidationFunc       string    `toml:"consolidation"`
	StartTime               int64     `toml:"start"`
	StopTime                int64     `toml:"stop"`
	StepTime                int64     `toml:"step"`
	XFilesFactor            float32   `toml:"xfiles"`
	HighPrecisionTimestamps bool      `toml:"precision"`
	Values                  []float64 `toml:"values"`
	AppliedFunctions        []string  `toml:"applied_functions"`
	RequestStartTime        int64     `toml:"req_start"`
	RequestStopTime         int64     `toml:"req_stop"`
}

// Render do /metrics/find/ request
// Valid formats are carbonapi_v3_pb. protobuf, pickle, json
func Render(client *http.Client, address string, format FormatType, targets []string, from, until int64) (string, []Metric, http.Header, error) {
	rUrl := "/render/"
	if format == FormatDefault {
		format = FormatPb_v3
	}

	queryParams := fmt.Sprintf("%s?format=%s, from=%d, until=%d, targets [%s]", rUrl, format.String(), from, until, strings.Join(targets, ","))
	if len(targets) == 0 {
		return queryParams, nil, nil, nil
	}

	if from <= 0 {
		return queryParams, nil, nil, ErrInvalidFrom
	}
	if until <= 0 {
		return queryParams, nil, nil, ErrInvalidUntil
	}
	fromStr := strconv.FormatInt(from, 10)
	untilStr := strconv.FormatInt(until, 10)

	u, err := url.Parse(address + rUrl)
	if err != nil {
		return queryParams, nil, nil, err
	}

	var v url.Values
	var reader io.Reader
	switch format {
	case FormatPb_v3:
		v = url.Values{
			"format": []string{format.String()},
		}
		u.RawQuery = v.Encode()

		var body []byte
		r := protov3.MultiFetchRequest{
			Metrics: make([]protov3.FetchRequest, len(targets)),
		}
		for i, target := range targets {
			r.Metrics[i] = protov3.FetchRequest{
				Name:           target,
				StartTime:      from,
				StopTime:       until,
				PathExpression: target,
			}
		}

		body, err = r.Marshal()
		if err != nil {
			return queryParams, nil, nil, err
		}
		if body != nil {
			reader = bytes.NewReader(body)
		}
	case FormatPb_v2, FormatProtobuf, FormatPickle, FormatJSON:
		v := url.Values{
			"format": []string{format.String()},
			"from":   []string{fromStr},
			"until":  []string{untilStr},
			"target": targets,
		}
		u.RawQuery = v.Encode()
	default:
		return queryParams, nil, nil, ErrUnsupportedFormat
	}

	req, err := http.NewRequest("GET", u.String(), reader)
	if err != nil {
		return queryParams, nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return queryParams, nil, nil, err
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return queryParams, nil, nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return queryParams, nil, resp.Header, nil
	} else if resp.StatusCode != http.StatusOK {
		return queryParams, nil, resp.Header, NewHttpError(resp.StatusCode, string(b))
	}

	var metrics []Metric

	switch format {
	case FormatPb_v3:
		var r protov3.MultiFetchResponse
		err = r.Unmarshal(b)
		if err != nil {
			return queryParams, nil, resp.Header, err
		}
		metrics = make([]Metric, 0, len(r.Metrics))
		for _, m := range r.Metrics {
			metrics = append(metrics, Metric{
				Name:                    m.Name,
				PathExpression:          m.PathExpression,
				ConsolidationFunc:       m.ConsolidationFunc,
				StartTime:               m.StartTime,
				StopTime:                m.StopTime,
				StepTime:                m.StepTime,
				XFilesFactor:            m.XFilesFactor,
				HighPrecisionTimestamps: m.HighPrecisionTimestamps,
				Values:                  m.Values,
				AppliedFunctions:        m.AppliedFunctions,
				RequestStartTime:        m.RequestStartTime,
				RequestStopTime:         m.StopTime,
			})
		}
	case FormatPb_v2, FormatProtobuf:
		var r protov2.MultiFetchResponse
		err = r.Unmarshal(b)
		if err != nil {
			return queryParams, nil, resp.Header, err
		}
		metrics = make([]Metric, 0, len(r.Metrics))
		for _, m := range r.Metrics {
			for i, a := range m.IsAbsent {
				if a {
					m.Values[i] = math.NaN()
				}
			}

			metrics = append(metrics, Metric{
				Name:      m.Name,
				StartTime: int64(m.StartTime),
				StopTime:  int64(m.StopTime),
				StepTime:  int64(m.StepTime),
				Values:    m.Values,
			})
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
			vals := m["values"].([]interface{})
			values := make([]float64, len(vals))
			for i, vv := range vals {
				if _, isNaN := vv.(pickle.None); isNaN {
					values[i] = math.NaN()
				} else {
					values[i] = vv.(float64)
				}
			}
			metrics = append(metrics, Metric{
				Name:           m["name"].(string),
				PathExpression: m["pathExpression"].(string),
				StartTime:      m["start"].(int64),
				StopTime:       m["end"].(int64),
				StepTime:       m["step"].(int64),
				Values:         values,
			})
		}
	default:
		return queryParams, nil, resp.Header, ErrUnsupportedFormat
	}

	return queryParams, metrics, resp.Header, nil
}
