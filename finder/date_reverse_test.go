package finder

import (
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
)

func TestDateFinderV3_whereFilter(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		from     int64
		until    int64
		want     string
		wantDate string
	}{
		{
			name:     "midnight at utc (direct)",
			query:    "test.metric*",
			from:     1668124800, // 2022-11-11 00:00:00 UTC
			until:    1668124810, // 2022-11-11 00:00:10 UTC
			want:     "(Level=2) AND (Path LIKE 'metric%' AND match(Path, '^metric([^.]*?)[.]test[.]?$'))",
			wantDate: "Date >='" + date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668124810) + "'",
		},
		{
			name:     "midnight at utc (reverse)",
			query:    "*test.metric",
			from:     1668124800, // 2022-11-11 00:00:00 UTC
			until:    1668124810, // 2022-11-11 00:00:10 UTC
			want:     "(Level=2) AND (Path LIKE 'metric.%' AND match(Path, '^metric[.]([^.]*?)test[.]?$'))",
			wantDate: "Date >='" + date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668124810) + "'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+" "+time.Unix(tt.from, 0).Format(time.RFC3339), func(t *testing.T) {
			f := NewDateFinderV3("http://localhost:8123/", "graphite_index", clickhouse.Options{}).(*DateFinderV3)
			got, gotDate := f.whereFilter(tt.query, tt.from, tt.until)
			if got.String() != tt.want {
				t.Errorf("DateFinderV3.whereFilter()[0] = %v, want %v", got, tt.want)
			}
			if gotDate.String() != tt.wantDate {
				t.Errorf("DateFinderV3.whereFilter()[1] = %v, want %v", gotDate, tt.wantDate)
			}
		})
	}
}
