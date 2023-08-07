[common]

[data]
path = "/etc/carbon-clickhouse/data"
chunk-interval = "1s"
chunk-auto-interval = ""

[upload.graphite_index]
type = "index"
table = "graphite_index"
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
cache-ttl = "1h"

[upload.graphite_tags]
type = "tagged"
table = "graphite_tags"
threads = 3
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
cache-ttl = "1h"

[upload.graphite_reverse]
type = "points-reverse"
table = "graphite_reverse"
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
zero-timestamp = false

[upload.graphite]
type = "points"
table = "graphite"
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
zero-timestamp = false

[tcp]
listen = ":2003"
enabled = true
drop-future = "0s"
drop-past = "0s"

[logging]
file = "/etc/carbon-clickhouse/carbon-clickhouse.log"
level = "debug"
