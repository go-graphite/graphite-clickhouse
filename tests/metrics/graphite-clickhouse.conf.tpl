[common]
listen = "{{ .GCH_ADDR }}"
max-cpu = 0
max-metrics-in-render-answer = 10000
max-metrics-per-target = 10000
headers-to-log = [ "X-Ctx-Carbonapi-Uuid" ]

[clickhouse]
url = "{{ .PROXY_URL }}/?max_rows_to_read=500000000&max_result_bytes=1073741824&readonly=2&log_queries=1"
data-timeout = "1s"

query-params = [
  {
    duration = "1h",
    url = "{{ .PROXY_URL }}/?max_rows_to_read=1&max_result_bytes=1&readonly=2&log_queries=1",
    data-timeout = "5s"
  },
  {
    duration = "7h",
    url = "{{ .PROXY_URL }}/?max_memory_usage=1&max_memory_usage_for_user=1&readonly=2&log_queries=1",
    data-timeout = "5s"
  }
]

index-table = "graphite_index"
index-use-daily = true
index-timeout = "1s"
internal-aggregation = false

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

[metrics]
# Graphite metrics
metric-endpoint = "127.0.0.1:2003"
statsd-endpoint = "127.0.0.1:8125"
metric-prefix = "DevOps.graphite.graphite-clickhouse.{host}"
metric-interval = "10s"
extended-stat = true
ranges = { "1d" = "24h", "7d" = "168h", "90d" = "2160h" }
