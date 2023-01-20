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
If executing max-concurrent requests, next request will be wait for free slot until index-timeout reached
If wait max-queries requests, for new request error returned immediately.

```
url = "http://graphite:qwerty@localhost:8123/?readonly=2&log_queries=1&max_rows_to_read=102400000&max_result_bytes=12800000&max_threads=2"
render-max-queries = 500
render-max-concurrent = 10
find-max-queries = 100
find-max-concurrent = 10
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
Only one tag used as filter for index field Tag1, see graphite_tagged table [structure](https://github.com/lomik/carbon-clickhouse#clickhouse-configuration)

So, if the first tag in filter is costly (poor selectivity), like environment (with several possible values), query perfomance will be degraded.
Tune this with `tagged-costs` options:

```
tagged-costs = {
    "environment" = { cost: 100 },
    "project" = { values-cost = { "HugeProject" = 90 } } # overwrite tag value cost for some value only
}
```

Default cost is 0 and positive or negative numbers can be used. So if environment is first tag filter in query, it will used as primary only if no other filters with equal operation. Costs from values-cost also applied to regex match or wilrdcarded equal.

## Carbonlink `[carbonlink]`
The configuration to get metrics from carbon-cache. See details in [graphite-web](https://graphite.readthedocs.io/en/latest/carbon-daemons.html#carbon-relay-py) documentation.

***TODO***: add the real use-case configuration.

## Logging `[logging]`
It's possible to set multiple loggers. See `Config` description in [config.go](https://github.com/lomik/zapwriter/blob/master/config.go) for details.

## Metrics `[metrics]`

Send internal metrics to graphite relay
 - `metric-endpoint`   - graphite relay address
 - `statsd-endpoint`   - StatsD aggregator address
 - `metric-interval`   - graphite metrics send interval
 - `metric-prefix`     - graphite metrics prefix 
 - `metric-timeout`    - graphite metrics send timeout
 - `ranges`            - separate stat for render query (and internal clickhouse queries) until-from ranges, for example { "1d" = "24h", "7d" = "168h", "90d" = "2160h" }
 - `request-buckets`   - request historgram buckets widths, by default [200, 500, 1000, 2000, 3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 60000]
 - `request-labels`    - optional request historgram buckets labels

[//]: # (!!!DO NOT EDIT FOLLOWING LINES!!!)

# Example

