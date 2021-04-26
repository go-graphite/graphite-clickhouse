package data

import (
	"errors"

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

// ToMultiFetchResponseV3 returns protobuf MultiFetchResponse message for given CHResponse
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

// ToMultiFetchResponseV3 returns protobuf MultiFetchResponse message for given CHResponses
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
