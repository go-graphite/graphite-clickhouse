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
}

// CHResponses is a slice of CHResponse
type CHResponses []CHResponse

// EmptyResponse returns an CHResponses with one element containing emptyData for the following encoding
func EmptyResponse() CHResponses { return CHResponses{{emptyData, 0, 0}} }

// ToMultiFetchResponseV2 returns protobuf v2pb.MultiFetchResponse message for given CHResponse
func (c *CHResponse) ToMultiFetchResponseV2() (*v2pb.MultiFetchResponse, error) {
	mfr := &v2pb.MultiFetchResponse{Metrics: make([]v2pb.FetchResponse, 0)}
	data := c.Data
	nextMetric := data.GroupByMetric()
	for {
		points := nextMetric()
		if len(points) == 0 {
			break
		}
		id := points[0].MetricID
		name := data.MetricName(id)
		step, err := data.GetStep(id)
		if err != nil {
			return nil, err
		}
		start, stop, count, getValue := point.FillNulls(points, uint32(c.From), uint32(c.Until), step)
		values := make([]float64, 0, count)
		isAbsent := make([]bool, 0, count)
		for {
			value, err := getValue()
			if err != nil {
				if errors.Is(err, point.ErrTimeGreaterStop) {
					break
				}
				// if err is not point.ErrTimeGreaterStop, the points are corrupted
				return nil, err
			}
			if math.IsNaN(value) {
				values = append(values, 0)
				isAbsent = append(isAbsent, true)
				continue
			}
			values = append(values, value)
			isAbsent = append(isAbsent, false)
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
	nextMetric := data.GroupByMetric()
	for {
		points := nextMetric()
		if len(points) == 0 {
			break
		}
		id := points[0].MetricID
		name := data.MetricName(id)
		consolidationFunc, err := data.GetAggregation(id)
		if err != nil {
			return nil, err
		}
		step, err := data.GetStep(id)
		if err != nil {
			return nil, err
		}
		start, stop, count, getValue := point.FillNulls(points, uint32(c.From), uint32(c.Until), step)
		values := make([]float64, 0, count)
		for {
			value, err := getValue()
			if err != nil {
				if errors.Is(err, point.ErrTimeGreaterStop) {
					break
				}
				// if err is not point.ErrTimeGreaterStop, the points are corrupted
				return nil, err
			}
			values = append(values, value)
		}
		for _, a := range data.AM.Get(name) {
			fr := v3pb.FetchResponse{
				Name:                    a.DisplayName,
				PathExpression:          a.Target,
				ConsolidationFunc:       consolidationFunc,
				StartTime:               int64(start),
				StopTime:                int64(stop),
				StepTime:                int64(step),
				XFilesFactor:            0,
				HighPrecisionTimestamps: false,
				Values:                  values,
				RequestStartTime:        c.From,
				RequestStopTime:         c.Until,
			}
			mfr.Metrics = append(mfr.Metrics, fr)
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
