package clickhouse

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_extractClickhouseError(t *testing.T) {
	tests := []struct {
		errStr      string
		wantStatus  int
		wantMessage string
	}{
		{
			errStr:      "clickhouse response status 500: Code: 158. DB::Exception: Received from host:9000. DB::Exception: Limit for rows (controlled by 'max_rows_to_read' setting) exceeded, max rows: 10.00, current rows: 8.19 thousand. (TOO_MANY_ROWS) (version 22.2.2.1)\n",
			wantStatus:  http.StatusForbidden,
			wantMessage: "Storage read limit for rows (controlled by 'max_rows_to_read' setting) exceeded, max rows: 10.00, current rows: 8.19 thousand. (TOO_MANY_ROWS)",
		},
		{

			errStr:      "clickhouse response status 500: Code: 158. DB::Exception: Limit for rows (controlled by 'max_rows_to_read' setting) exceeded, max rows: 1.00, current rows: 50.00. (TOO_MANY_ROWS) (version 22.1.3.7 (official build))\n",
			wantStatus:  http.StatusForbidden,
			wantMessage: "Storage read limit for rows (controlled by 'max_rows_to_read' setting) exceeded, max rows: 1.00, current rows: 50.00. (TOO_MANY_ROWS)",
		},
		{
			errStr:      "Malformed response from clickhouse: Code: 241. DB::Exception: Received from host:9000. DB::Exception: Memory limit (for query) exceeded: would use 77.20 GiB (attempt to allocate chunk of 13421776 bytes), maximum: 4.51 GiB: While executing AggregatingTransform. (MEMORY_LIMIT_EXCEEDED) (version 22.2.2.1)\n",
			wantStatus:  http.StatusForbidden,
			wantMessage: "Storage read limit for memory",
		},
		{
			errStr:      "Malformed response from clickhouse : Code: 241. DB::Exception: Received from host:9000. DB::Exception: Memory limit (for query) exceeded: would use 6.66 GiB (attempt to allocate chunk of 8537964 bytes), maximum: 4.51 GiB: (avg_value_size_hint = 208.48085594177246, avg_chars_size = 240.57702713012694, limit = 32768): ... : While executing MergeTreeThread. (MEMORY_LIMIT_EXCEEDED)",
			wantStatus:  http.StatusForbidden,
			wantMessage: "Storage read limit for memory",
		},
		{
			errStr:      "clickhouse response status 404: Code: 60. DB::Exception: Table default.graphite_index does not exist. (UNKNOWN_TABLE) (version 23.12.6.19 (official build))\n",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "Storage default tables damaged",
		},
		{
			errStr:      "Other error",
			wantStatus:  http.StatusServiceUnavailable,
			wantMessage: "Storage unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.errStr, func(t *testing.T) {
			gotStatus, gotMessage := extractClickhouseError(tt.errStr)
			assert.Equal(t, tt.wantStatus, gotStatus)
			assert.Equal(t, tt.wantMessage, gotMessage)
		})
	}
}
