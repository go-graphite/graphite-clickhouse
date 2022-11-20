//go:build !noprom
// +build !noprom

package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type nopGatherer struct{}

var _ prometheus.Gatherer = &nopGatherer{}

func (*nopGatherer) Gather() ([]*dto.MetricFamily, error) {
	return []*dto.MetricFamily{}, nil
}
