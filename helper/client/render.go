package client

import (
	"bytes"
	"encoding/json"
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

var (
	ErrInvalidFrom  = errors.New("invalid from")
	ErrInvalidUntil = errors.New("invalid until")
)

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
func Render(client *http.Client, address string, format FormatType, targets []string, filteringFunctions []*protov3.FilteringFunction, maxDataPoints, from, until int64) (string, []Metric, http.Header, error) {
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
	maxDataPointsStr := strconv.FormatInt(maxDataPoints, 10)

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
				Name:            target,
				StartTime:       from,
				StopTime:        until,
				PathExpression:  target,
				FilterFunctions: filteringFunctions,
				MaxDataPoints:   maxDataPoints,
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
			"format":        []string{format.String()},
			"from":          []string{fromStr},
			"until":         []string{untilStr},
			"target":        targets,
			"maxDataPoints": []string{maxDataPointsStr},
		}
		u.RawQuery = v.Encode()
	default:
		return queryParams, nil, nil, ErrUnsupportedFormat
	}

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

	metrics, err := Decode(b, format)
	if err != nil {
		return queryParams, nil, resp.Header, err
	}

	return queryParams, metrics, resp.Header, nil
}

// Decode converts data in the give format to a Metric
func Decode(b []byte, format FormatType) ([]Metric, error) {
	var (
		metrics []Metric
		err     error
	)

	switch format {
	case FormatPb_v3:
		var r protov3.MultiFetchResponse

		err = r.Unmarshal(b)
		if err != nil {
			return nil, err
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
			return nil, err
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
			return nil, err
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
	case FormatJSON:
		var r jsonResponse

		err = json.Unmarshal(b, &r)
		if err != nil {
			return nil, err
		}

		metrics = make([]Metric, 0, len(r.Metrics))

		for _, m := range r.Metrics {
			values := make([]float64, len(m.Values))

			for i, v := range m.Values {
				if v == nil {
					values[i] = math.NaN()
				} else {
					values[i] = *v
				}
			}

			metrics = append(metrics, Metric{
				Name:           m.Name,
				PathExpression: m.PathExpression,
				StartTime:      m.StartTime,
				StopTime:       m.StopTime,
				StepTime:       m.StepTime,
				Values:         values,
			})
		}
	default:
		return nil, ErrUnsupportedFormat
	}

	return metrics, nil
}

// jsonResponse is a simple struct to decode JSON responses for testing purposes
type jsonResponse struct {
	Metrics []jsonMetric `json:"metrics"`
}

type jsonMetric struct {
	Name           string     `json:"name"`
	PathExpression string     `json:"pathExpression"`
	Values         []*float64 `json:"values"`
	StartTime      int64      `json:"startTime"`
	StopTime       int64      `json:"stopTime"`
	StepTime       int64      `json:"stepTime"`
}
