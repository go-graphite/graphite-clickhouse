[//]: # (This file is built out of deploy/doc/config.md, please do not edit it manually)  
[//]: # (To rebuild it run `make config`)

# Configuration

## Common  `[common]`

### Finder cache

Specify what storage to use for finder cache. This cache stores finder results (metrics find/tags autocomplete/render).

Supported cache types:
 - `mem` - will use integrated in-memory cache. Not distributed. Fast.
 - `memcache` - will use specified memcache servers. Could be shared. Slow.
 - `null` - disable cache

Extra options:
 - `size_mb` - specify max size of cache, in MiB
 - `defaultTimeoutSec` - specify default cache ttl.
 - `shortTimeoutSec` - cache ttl for short duration intervals of render queries (duration <= shortDuration && now-until <= 61) (if 0, disable this cache)
 - `findTimeoutSec` - cache ttl for finder/tags autocompleter queries (if 0, disable this cache)
 - `shortDuration` - maximum duration for render queries, which use shortTimeoutSec duration

### Example
```yaml
[common.find-cache]
type = "memcache"
size_mb = 0
memcachedServers = [ "127.0.0.1:1234", "127.0.0.2:1235" ]
defaultTimeoutSec = 10800
shortTimeoutSec = 300
findTimeoutSec = 600
```

## Feature flags `[feature-flags]`

`use-carbon-behaviour=true`.

- Tagged terms with `=` operator and empty value (e.g. `t=`) match all metrics that don't have that tag.

`dont-match-missing-tags=true`.

- Tagged terms with `!=`, `!=~` operators only match metrics that have that tag.

### Examples

Given tagged metrics:
```
metric.two;env=prod
metric.one;env=stage;dc=mydc1
metric.one;env=prod;dc=otherdc1
```
| Target                      | use-carbon-behaviour | Matched metrics                                   |
|-----------------------------|----------------------|---------------------------------------------------|
| seriesByTag('dc=')          | false                | -                                                 |
| seriesByTag('dc=')          | true                 | metric.two;env=prod                               |

| Target                   | dont-match-missing-tags | Matched metrics                                        |
|--------------------------|-------------------------|--------------------------------------------------------|
| seriesByTag('dc!=mydc1') | false                   | metric.two;env=prod<br>metric.one;env=prod;dc=otherdc1 |
| seriesByTag('dc!=mydc1') | true                    | metric.one;env=prod;dc=otherdc1                        |
| seriesByTag('dc!=~otherdc') | false                | metric.two;env=prod<br>metric.one;env=stage;dc=mydc1 |
| seriesByTag('dc!=~otherdc') | true                 | metric.one;env=stage;dc=mydc1                     |

## ClickHouse `[clickhouse]`

### URL `url`
Detailed explanation of ClickHouse HTTP interface is given in [documentation](https://clickhouse.tech/docs/en/interfaces/http). It's recommended to create a dedicated read-only user for graphite-clickhouse.

Example: `url = "http://graphite:qwerty@localhost:8123/?readonly=2&log_queries=1"`

Some useful parameters:

- [log_queries=1](https://clickhouse.tech/docs/en/operations/settings/settings/#settings-log-queries): all queries will be logged in the `system.query_log` table. Useful for debug.
- [readonly=2](https://clickhouse.tech/docs/en/operations/settings/permissions-for-queries/#settings_readonly): do not change data on the server
- [max_rows_to_read=200000000](https://clickhouse.tech/docs/en/operations/settings/query-complexity/#max-rows-to-read): useful if you want to prevent too broad requests
- [cancel_http_readonly_queries_on_client_close=1](https://clickhouse.tech/docs/en/operations/settings/settings/#cancel-http-readonly-queries-on-client-close): cancel DB query when request is canceled.

All these and more settings can be set in clickhouse-server configuration as user's profile settings.

Useless settings:

- `max_query_size`: at the moment [external data](https://clickhouse.tech/docs/en/engines/table-engines/special/external-data/) is used, the query length is relatively small and always less than the default [262144](https://clickhouse.tech/docs/en/operations/settings/settings/#settings-max_query_size)
- `max_ast_elements`: the same
- `max_execution_time`: with `cancel_http_readonly_queries_on_client_close=1` and `data-timeout = "1m"` it's already covered.

### Query multi parameters (for overwrite default url and data-timeout)

For queries with duration (until - from) >= 72 hours, use custom url and data-timeout

```
url = "http://graphite:qwerty@localhost:8123/?readonly=2&log_queries=1&max_rows_to_read=102400000&max_result_bytes=12800000&max_threads=2"
data-timeout = "30s"

query-params = [
  {
    duration = "72h",
    url = "http://graphite:qwerty@localhost:8123/?readonly=2&log_queries=1&max_rows_to_read=1024000000&max_result_bytes=128000000&max_threads=1",
    data-timeout = "60s"
  }
]
```

### Query limiter for prevent database overloading (limit concurrent/maximum incomming requests)

For prevent database overloading incomming requests (render/find/autocomplete) can be limited.
If wait max-queries requests, for new request error returned immediately.
If executing concurrent-queries requests, next request will be wait for free slot until index-timeout reached
adaptive-queries prevent overload with load average check if  graphite-clickhouse run on one host with clickhouse
Real queries will be concurrent-queries + adaptive-queries * (1 / normalized_load_avg - 1).
If normalized_load_avg > 0.9, limit will be concurrent-queries.
```
url = "http://graphite:qwerty@localhost:8123/?readonly=2&log_queries=1&max_rows_to_read=102400000&max_result_bytes=12800000&max_threads=2"
render-max-queries = 500
render-max-concurrent = 10
find-max-queries = 100
find-concurrent-queries = 10
tags-max-queries = 100
tags-max-concurrent = 10

query-params = [
  {
    duration = "72h",
    url = "http://graphite:qwerty@localhost:8123/?readonly=2&log_queries=1&max_rows_to_read=1024000000&max_result_bytes=128000000&max_threads=1",
    data-timeout = "60s"
    max-queries = 100,
    max-concurrent = 4
  }
]

user-limits = {
  "alerting" = {
    max-queries = 100,
    max-concurrent = 5
  }
}

```

### Index table
See [index table](./index-table.md) documentation for details.

### Index reversed queries tuning
By default the daemon decides to make a direct or reversed request to the [index table](./index-table.md) based on a first and last glob node in the metric. It choose the most long path to reduce readings. Additional examples can be found in [tests](../finder/index_test.go).

You can overwrite automatic behavior with `index-reverse`. Valid values are `"auto", direct, "reversed"`

If you need fine tuning for different paths, you can use `[[clickhouse.index-reverses]]` to set behavior per metrics' `prefix`, `suffix` or `regexp`.

### Tags table
By default, tags are stored in the tagged-table on the daily basis. If a metric set doesn't change much, that leads to situation when the same data stored multiple times.
To prevent uncontrolled growth and reduce the amount of data stored in the tagged-table, the `tagged-use-daily` parameter could be set to `false` and table definition could be changed to something like:
```
CREATE TABLE graphite_tagged (
  Date Date,
  Tag1 String,
  Path String,
  Tags Array(String),
  Version UInt32
) ENGINE = ReplacingMergeTree(Date)
ORDER BY (Tag1, Path);
```

For restrict costly seriesByTag (may be like `seriesByTag('name=~test.*.*.rabbitmq_overview.connections')` or `seriesByTag('name=test.*.*.rabbitmq_overview.connections')`) use tags-min-in-query parameter.
For restrict costly autocomplete queries use tags-min-in-autocomplete parameter.

set for require at minimum 1 eq argument (without wildcards)
`tags-min-in-query=1`


`ReplacingMergeTree(Date)` prevent broken tags autocomplete with default `ReplacingMergeTree(Version)`, when write to the past.

### ClickHouse aggregation
For detailed description of `max-data-points` and `internal-aggregation` see [aggregation documentation](./aggregation.md).

## Data tables `[[data-table]]`

### Rollup
The rollup configuration is used for a proper  metrics pre-aggregation. It contains two rules types:

- retention for point per time range
- aggregation function for a values

Historically, the way to define the config was `rollup-conf = "/path/to/the/conf/with/graphite_rollup.xml"`. The format is the same as [graphite_rollup](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/graphitemergetree/#rollup-configuration) scheme for ClickHouse server.

For a quite long time it's recommended to use `rollup-conf = "auto"` to get the configuration from remote ClickHouse server. It will update itself on each `rollup-auto-interval` (1 minute by default) or once on startup if set to "0s".

If you don't use a `GraphiteMergeTree` family engine, you can still use `rollup-conf = "auto"` by setting `rollup-auto-table="graphiteMergeTreeTable"` and get the proper config. In this case `graphiteMergeTreeTable` is a dummy table associated with proper [graphite_rollup](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/graphitemergetree/#rollup-configuration). The cases when you may need it:

- ReplacingMergeTree engine
- Distributed engine
- Materialized view

It's possible as well to set `rollup-conf = "none"`. Then values from `rollup-default-precision` and `rollup-default-function` will be used.

#### Additional rollup tuning for reversed data tables
When `reverse = true` is set for data-table, there are two possibles cases for [graphite_rollup](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/graphitemergetree/#rollup-configuration):

- Original regexps are used, like `^level_one.level_two.suffix$`
- Reversed regexps are used, like `^suffix.level_two.level_one$`

Depends on it for having a proper retention and aggregation you must additionally set `rollup-use-reverted = true` for the first case and `rollup-use-reverted = false` for the second.

#### Additional tuning tagged find for seriesByTag and autocomplete
Only one tag used as filter for index field Tag1, see graphite_tagged table [structure](https://github.com/lomik/

To always choose the best Tag1 you can set the parameter `tag1-count-table = <table_name>`. The value should be a table in clickhouse that has columns (Date, Tag1, Count) similar to the graphite_tagged table. The table can be defined like this:

```
CREATE TABLE IF NOT EXISTS default.tag1_count_per_day
(
  Date Date,
  Tag1 String,
  Count UInt64
)
ENGINE = SummingMergeTree
ORDER BY (Date, Tag1);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.tag1_count_per_day_mv TO default.tag1_count_per_day AS
SELECT Date AS Date,
       Tag1 AS Tag1,
       count(*) AS Count
FROM default.graphite_tags
GROUP BY (Date, Tag1);
```

Here we additionally create a materialized view to automatically save the quantities of rows with each unique Tag1 as the metrics are being written.
graphite-clickhouse will query this table when it tries to decide which tag should be used when querying graphite_tagged table.
Overall using this parameter will somewhat increase writing load but can improve reading tagged metrics greatly in some cases.

Note that this option only works for terms with '=' operator in them.

```toml
[common]
 # general listener
 listen = ":9090"
 # listener to serve /debug/pprof requests. '-pprof' argument overrides it
 pprof-listen = ""
 max-cpu = 1
 # limit number of results from find query, 0=unlimited
 max-metrics-in-find-answer = 0
 # limit numbers of queried metrics per target in /render requests, 0 or negative = unlimited
 max-metrics-per-target = 15000
 # if true, always return points for all metrics, replacing empty results with list of NaN
 append-empty-series = false
 # daemon returns empty response if query matches any of regular expressions
 # target-blacklist = []
 # daemon will return the freed memory to the OS when it>0
 memory-return-interval = "0s"
 # additional request headers to log
 headers-to-log = []
 # service discovery base weight (on idle)
 base_weight = 0
 # service discovery degraded load avg multiplier (if normalized load avg > degraged_load_avg) (default 4.0)
 degraged-multiply = 4.0
 # service discovery normilized load avg degraded point (default 1.0)
 degraged-load-avg = 1.0
 # service discovery type
 service-discovery-type = 0
 # service discovery address (consul)
 service-discovery = ""
 # service discovery namespace (graphite by default)
 service-discovery-ns = ""
 # service discovery datacenters (first - is primary, in other register as backup)
 service-discovery-ds = []
 # service discovery expire duration for cleanup (minimum is 24h, if enabled)
 service-discovery-expire = "0s"

 # find/tags cache config
 [common.find-cache]
  # cache type
  type = "null"
  # cache size
  size-mb = 0
  # memcached servers
  memcached-servers = []
  # default cache ttl
  default-timeout = 0
  # short-time cache ttl
  short-timeout = 0
  # finder/tags autocompleter cache ttl
  find-timeout = 0
  # maximum diration, used with short_timeout
  short-duration = "0s"
  # offset beetween now and until for select short cache timeout
  short-offset = 0

[feature-flags]
 # if true, prefers carbon's behaviour on how tags are treated
 use-carbon-behaviour = false
 # if true, seriesByTag terms containing '!=' or '!=~' operators will not match metrics that don't have the tag at all
 dont-match-missing-tags = false

[metrics]
 # graphite relay address
 metric-endpoint = ""
 # statsd server address
 statsd-endpoint = ""
 # Extended metrics
 extended-stat = false
 # graphite metrics send interval
 metric-interval = "0s"
 # graphite metrics send timeout
 metric-timeout = "0s"
 # graphite metrics prefix
 metric-prefix = ""
 # Request historgram buckets widths
 request-buckets = []
 # Request historgram buckets labels
 request-labels = []

 # Additional separate stats for until-from ranges
 [metrics.ranges]

 # Additional separate stats for until-from find ranges
 [metrics.find-ranges]

[clickhouse]
 # default url, see https://clickhouse.tech/docs/en/interfaces/http. Can be overwritten with query-params
 url = "http://localhost:8123?cancel_http_readonly_queries_on_client_close=1"
 # default total timeout to fetch data, can be overwritten with query-params
 data-timeout = "1m0s"
 # Max queries to render queiries
 render-max-queries = 0
 # Concurrent queries to render queiries
 render-concurrent-queries = 0
 # Render adaptive queries (based on load average) for increase/decrease concurrent queries
 render-adaptive-queries = 0
 # Max queries for find queries
 find-max-queries = 0
 # Find concurrent queries for find queries
 find-concurrent-queries = 0
 # Find adaptive queries (based on load average) for increase/decrease concurrent queries
 find-adaptive-queries = 0
 # Max queries for tags queries
 tags-max-queries = 0
 # Concurrent queries for tags queries
 tags-concurrent-queries = 0
 # Tags adaptive queries (based on load average) for increase/decrease concurrent queries
 tags-adaptive-queries = 0
 # If a wildcard appears both at the start and the end of a plain query at a distance (in terms of nodes) less than wildcard-min-distance, then it will be discarded. This parameter can be used to discard expensive queries.
 wildcard-min-distance = 0
 # Plain queries like '{first,second}.custom.metric.*' are also a subject to wildcard-min-distance restriction. But can be split into 2 queries: 'first.custom.metric.*', 'second.custom.metric.*'. Note that: only one list will be split; if there are wildcard in query before (after) list then reverse (direct) notation will be preferred; if there are wildcards before and after list, then query will not be split
 try-split-query = false
 # Used only if try-split-query is true. Query that contains list will be split if its (list) node index is less or equal to max-node-to-split-index. By default is 0. It is recommended to have this value set to 2 or 3 and increase it very carefully, because 3 or 4 plain nodes without wildcards have good selectivity
 max-node-to-split-index = 0
 # Minimum tags in seriesByTag query
 tags-min-in-query = 0
 # Minimum tags in autocomplete query
 tags-min-in-autocomplete = 0

 # customized query limiter for some users
 # [clickhouse.user-limits]
 # Date format (default, utc, both)
 date-format = ""
 # see doc/index-table.md
 index-table = "graphite_index"
 index-use-daily = true
 # see doc/config.md
 index-reverse = "auto"

 # [[clickhouse.index-reverses]]
  # rule is used when the target suffix is matched
  # suffix = "suffix"
  # same as index-reverse
  # reverse = "auto"

 # [[clickhouse.index-reverses]]
  # rule is used when the target prefix is matched
  # prefix = "prefix"
  # same as index-reverse
  # reverse = "direct"

 # [[clickhouse.index-reverses]]
  # rule is used when the target regex is matched
  # regex = "regex"
  # same as index-reverse
  # reverse = "reversed"
 # total timeout to fetch series list from index
 index-timeout = "1m0s"
 # 'tagged' table from carbon-clickhouse, required for seriesByTag
 tagged-table = "graphite_tagged"
 # Table that contains the total amounts of each tag-value pair. It is used to avoid usage of high cardinality tag-value pairs when querying TaggedTable. If left empty, basic sorting will be used. See more detailed description in doc/config.md
 tags-count-table = ""
 # or how long the daemon will query tags during autocomplete
 tagged-autocomplete-days = 7
 # whether to use date filter when searching for the metrics in the tagged-table
 tagged-use-daily = true

 # costs for tags (for tune which tag will be used as primary), by default is 0, increase for costly (with poor selectivity) tags
 # [clickhouse.tagged-costs]
 # old index table, DEPRECATED, see description in doc/config.md
 # tree-table = ""
 # reverse-tree-table = ""
 # date-tree-table = ""
 # date-tree-table-version = 0
 # tree-timeout = "0s"
 # is not recommended to use, https://github.com/lomik/graphite-clickhouse/wiki/TagsRU
 # tag-table = ""
 # add extra prefix (directory in graphite) for all metrics, w/o trailing dot
 extra-prefix = ""
 # TCP connection timeout
 connect-timeout = "1s"
 # will be removed in 0.14
 # data-table = ""
 # rollup-conf = "auto"
 # max points per metric when internal-aggregation=true
 max-data-points = 1048576
 # ClickHouse-side aggregation, see doc/aggregation.md
 internal-aggregation = true

 # mTLS HTTPS configuration for connecting to clickhouse server
 # [clickhouse.tls]
  # ca-cert = []
  # client-auth = ""
  # server-name = ""
  # min-version = ""
  # max-version = ""
  # insecure-skip-verify = false
  # curves = []
  # cipher-suites = []

[[data-table]]
 # data table from carbon-clickhouse
 table = "graphite_data"
 # if it stores direct or reversed metrics
 reverse = false
 # maximum age stored in the table
 max-age = "0s"
 # minimum age stored in the table
 min-age = "0s"
 # maximum until-from interval allowed for the table
 max-interval = "0s"
 # minimum until-from interval allowed for the table
 min-interval = "0s"
 # table allowed only if any metrics in target matches regexp
 target-match-any = ""
 # table allowed only if all metrics in target matches regexp
 target-match-all = ""
 # custom rollup.xml file for table, 'auto' and 'none' are allowed as well
 rollup-conf = "auto"
 # custom table for 'rollup-conf=auto', useful for Distributed or MatView
 rollup-auto-table = ""
 # rollup update interval for 'rollup-conf=auto'
 rollup-auto-interval = "1m0s"
 # is used when none of rules match
 rollup-default-precision = 0
 # is used when none of rules match
 rollup-default-function = ""
 # should be set to true if you don't have reverted regexps in rollup-conf for reversed tables
 rollup-use-reverted = false
 # valid values are 'graphite' of 'prometheus'
 context = []

# is not recommended to use, https://github.com/lomik/graphite-clickhouse/wiki/TagsRU
# [tags]
 # rules = ""
 # date = ""
 # extra-where = ""
 # input-file = ""
 # output-file = ""
 # number of threads for uploading tags to clickhouse (1 by default)
 # threads = 1
 # compression method for tags before sending them to clickhouse (i.e. content encoding): gzip (default), none, zstd
 # compression = "gzip"
 # fixed tags version for testing purposes (by default the current timestamp is used for each upload)
 # version = 0
 # number of chunks for selecting metrics from clickhouse (10 by default)
 # select-chunks-count = 0

[carbonlink]
 server = ""
 threads-per-request = 10
 connect-timeout = "50ms"
 query-timeout = "50ms"
 # timeout for querying and parsing response
 total-timeout = "500ms"

[prometheus]
 # listen addr for prometheus ui and api
 listen = ":9092"
 # allows to set URL for redirect manually
 external-url = ""
 page-title = "Prometheus Time Series Collection and Processing Server"
 lookback-delta = "5m0s"
 # concurrently handled remote read requests
 remote-read-concurrency-limit = 10

# see doc/debugging.md
[debug]
 # the directory for additional debug output
 directory = ""
 # permissions for directory, octal value is set as 0o755
 directory-perm = 493
 # permissions for directory, octal value is set as 0o640
 external-data-perm = 0

[[logging]]
 # handler name, default empty
 logger = ""
 # '/path/to/filename', 'stderr', 'stdout', 'empty' (=='stderr'), 'none'
 file = "/var/log/graphite-clickhouse/graphite-clickhouse.log"
 # 'debug', 'info', 'warn', 'error', 'dpanic', 'panic', and 'fatal'
 level = "info"
 # 'json' or 'console'
 encoding = "mixed"
 # 'millis', 'nanos', 'epoch', 'iso8601'
 encoding-time = "iso8601"
 # 'seconds', 'nanos', 'string'
 encoding-duration = "seconds"
 # passed to time.ParseDuration
 sample-tick = ""
 # first n messages logged per tick
 sample-initial = 0
 # every m-th message logged thereafter per tick
 sample-thereafter = 0
```
