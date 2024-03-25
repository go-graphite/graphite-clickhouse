[common]
listen = "{{ .GCH_ADDR }}"
max-cpu = 0
max-metrics-in-render-answer = 10000
max-metrics-per-target = 10000
headers-to-log = [ "X-Ctx-Carbonapi-Uuid" ]

[feature-flags]
use-carbon-behaviour = true

[clickhouse]
url = "{{ .CLICKHOUSE_URL }}/?max_rows_to_read=500000000&max_result_bytes=1073741824&readonly=2&log_queries=1"
data-timeout = "30s"

index-table = "graphite_index"
index-use-daily = true
index-timeout = "1m"
internal-aggregation = true

tagged-table = "graphite_tags"
tagged-autocomplete-days = 1

[[data-table]]
# # clickhouse table name
table = "graphite"
# # points in table are stored with reverse path
reverse = false
rollup-conf = "auto"

[[logging]]
logger = ""
file = "{{ .GCH_DIR }}/graphite-clickhouse.log"
level = "info"
encoding = "json"
encoding-time = "iso8601"
encoding-duration = "seconds"
