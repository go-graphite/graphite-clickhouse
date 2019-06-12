package prometheus

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return time.Unix(t.Unix(), 0), nil // convert to local
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func (h *Handler) seriesV1(w http.ResponseWriter, r *http.Request) {
	// logger := log.FromContext(r.Context())

	if err := r.ParseForm(); err != nil {
		http.Error(w, "error parsing form values", http.StatusBadRequest)
		return
	}
	if len(r.Form["match[]"]) == 0 {
		http.Error(w, "no match[] parameter provided", http.StatusBadRequest)
		return
	}

	var start time.Time
	if t := r.FormValue("start"); t != "" {
		var err error
		start, err = parseTime(t)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		start = time.Now().AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays)
	}

	var end time.Time
	if t := r.FormValue("end"); t != "" {
		var err error
		end, err = parseTime(t)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		end = time.Now()
	}

	matchWhere := finder.NewWhere()

	// var matcherSets [][]*labels.Matcher
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mw, err := wherePromQL(matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		matchWhere.Or(mw)
	}

	where := finder.NewWhere()
	where.Andf(
		"Date >='%s' AND Date <= '%s'",
		start.Format("2006-01-02"),
		end.Format("2006-01-02"),
	)
	where.And(matchWhere.String())

	sql := fmt.Sprintf(
		"SELECT Tags FROM %s WHERE %s GROUP BY Tags FORMAT JSON",
		h.config.ClickHouse.TaggedTable,
		where.String(),
	)
	body, err := clickhouse.Query(
		r.Context(),
		h.config.ClickHouse.Url,
		sql,
		h.config.ClickHouse.TaggedTable,
		clickhouse.Options{
			Timeout:        h.config.ClickHouse.IndexTimeout.Value(),
			ConnectTimeout: h.config.ClickHouse.ConnectTimeout.Value(),
		},
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := struct {
		Data []struct {
			Tags []string `json:"Tags"`
		} `json:"data"`
	}{}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result := []map[string]string{}
	for _, d := range resp.Data {
		m := make(map[string]string)
		for _, t := range d.Tags {
			p := strings.IndexByte(t, '=')
			if p > 0 {
				m[t[:p]] = t[p+1:]
			}
		}
		result = append(result, m)
	}

	b, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   result,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}
