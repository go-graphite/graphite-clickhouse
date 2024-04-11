package data

import (
	"errors"
	"math"

	v2pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"

	"github.com/lomik/graphite-clickhouse/helper/point"
)

// CHResponse contains the parsed Data and From/Until timestamps
type CHResponse struct {
	Data  *Data
	From  int64
	Until int64
	// if true, return points for all metrics, replacing empty results with list of NaN
	AppendOutEmptySeries bool
	AppliedFunctions     map[string][]string
}

// CHResponses is a slice of CHResponse
type CHResponses []CHResponse

// EmptyResponse returns an CHResponses with one element containing emptyData for the following encoding
func EmptyResponse() CHResponses { return CHResponses{{Data: emptyData}} }

// ToMultiFetchResponseV2 returns protobuf v2pb.MultiFetchResponse message for given CHResponse
func (c *CHResponse) ToMultiFetchResponseV2() (*v2pb.MultiFetchResponse, error) {
	mfr := &v2pb.MultiFetchResponse{Metrics: make([]v2pb.FetchResponse, 0)}
	data := c.Data

	addResponse := func(name string, step uint32, points []point.Point) error {
		from, until := uint32(c.From), uint32(c.Until)
		start, stop, count, getValue := point.FillNulls(points, from, until, step)
		values := make([]float64, 0, count)
		isAbsent := make([]bool, 0, count)
		for {
			value, err := getValue()
			if err != nil {
				if errors.Is(err, point.ErrTimeGreaterStop) {
					break
				}
				// if err is not point.ErrTimeGreaterStop, the points are corrupted
				return err
			}
			if math.IsNaN(value) {
				values = append(values, 0)
				isAbsent = append(isAbsent, true)
			} else {
				values = append(values, value)
				isAbsent = append(isAbsent, false)
			}
		}
		for _, a := range data.AM.Get(name) {
			fr := v2pb.FetchResponse{
				Name:      a.DisplayName,
				StartTime: int32(start),
				StopTime:  int32(stop),
				StepTime:  int32(step),
				Values:    values,
				IsAbsent:  isAbsent,
			}
			mfr.Metrics = append(mfr.Metrics, fr)
		}
		return nil
	}

	// process metrics with points
	writtenMetrics := make(map[string]struct{})
	nextMetric := data.GroupByMetric()
	for {
		points := nextMetric()
		if len(points) == 0 {
			break
		}
		id := points[0].MetricID
		name := data.MetricName(id)
		writtenMetrics[name] = struct{}{}
		step, err := data.GetStep(id)
		if err != nil {
			return nil, err
		}
		if err := addResponse(name, step, points); err != nil {
			return nil, err
		}
	}
	// process metrics with no points
	if c.AppendOutEmptySeries && len(writtenMetrics) < data.AM.Len() && data.CommonStep > 0 {
		for _, metricName := range data.AM.Series(false) {
			if _, done := writtenMetrics[metricName]; !done {
				err := addResponse(metricName, uint32(data.CommonStep), []point.Point{})
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return mfr, nil
}

// ToMultiFetchResponseV2 returns protobuf v2pb.MultiFetchResponse message for given CHResponses
func (cc *CHResponses) ToMultiFetchResponseV2() (*v2pb.MultiFetchResponse, error) {
	mfr := &v2pb.MultiFetchResponse{Metrics: make([]v2pb.FetchResponse, 0)}
	for _, c := range *cc {
		m, err := c.ToMultiFetchResponseV2()
		if err != nil {
			return nil, err
		}
		mfr.Metrics = append(mfr.Metrics, m.Metrics...)
	}
	return mfr, nil
}

// ToMultiFetchResponseV3 returns protobuf v3pb.MultiFetchResponse message for given CHResponse
func (c *CHResponse) ToMultiFetchResponseV3() (*v3pb.MultiFetchResponse, error) {
	mfr := &v3pb.MultiFetchResponse{Metrics: make([]v3pb.FetchResponse, 0)}
	data := c.Data
	addResponse := func(name, function string, step uint32, points []point.Point) error {
		from, until := uint32(c.From), uint32(c.Until)
		start, stop, count, getValue := point.FillNulls(points, from, until, step)
		values := make([]float64, 0, count)
		for {
			value, err := getValue()
			if err != nil {
				if errors.Is(err, point.ErrTimeGreaterStop) {
					break
				}
				// if err is not point.ErrTimeGreaterStop, the points are corrupted
				return err
			}
			values = append(values, value)
		}
		for _, a := range data.AM.Get(name) {
			fr := v3pb.FetchResponse{
				Name:                    a.DisplayName,
				PathExpression:          a.Target,
				ConsolidationFunc:       function,
				StartTime:               int64(start),
				StopTime:                int64(stop),
				StepTime:                int64(step),
				XFilesFactor:            0,
				HighPrecisionTimestamps: false,
				Values:                  values,
				AppliedFunctions:        c.AppliedFunctions[a.Target],
				RequestStartTime:        c.From,
				RequestStopTime:         c.Until,
			}
			mfr.Metrics = append(mfr.Metrics, fr)
		}
		return nil
	}

	// process metrics with points
	writtenMetrics := make(map[string]struct{})
	nextMetric := data.GroupByMetric()
	for {
		points := nextMetric()
		if len(points) == 0 {
			break
		}
		id := points[0].MetricID
		name := data.MetricName(id)
		writtenMetrics[name] = struct{}{}
		consolidationFunc, err := data.GetAggregation(id)
		if err != nil {
			return nil, err
		}
		step, err := data.GetStep(id)
		if err != nil {
			return nil, err
		}
		if err := addResponse(name, consolidationFunc, step, points); err != nil {
			return nil, err
		}
	}
	// process metrics with no points
	if c.AppendOutEmptySeries && len(writtenMetrics) < data.AM.Len() && data.CommonStep > 0 {
		for _, metricName := range data.AM.Series(false) {
			if _, done := writtenMetrics[metricName]; !done {
				err := addResponse(metricName, "any", uint32(data.CommonStep), []point.Point{})
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return mfr, nil
}

// ToMultiFetchResponseV3 returns protobuf v3pb.MultiFetchResponse message for given CHResponses
func (cc *CHResponses) ToMultiFetchResponseV3() (*v3pb.MultiFetchResponse, error) {
	mfr := &v3pb.MultiFetchResponse{Metrics: make([]v3pb.FetchResponse, 0)}
	for _, c := range *cc {
		m, err := c.ToMultiFetchResponseV3()
		if err != nil {
			return nil, err
		}
		mfr.Metrics = append(mfr.Metrics, m.Metrics...)
	}
	return mfr, nil
}
