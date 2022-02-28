[//]: # (This file is built out of deploy/doc/config.md, please do not edit it manually)  
[//]: # (To rebuild it run `make config`)

# Configuration

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

### Index table
See [index table](./index-table.md) documentation for details.

### Index reversed queries tuning
By default the daemon decides to make a direct or reversed request to the [index table](./index-table.md) based on a first and last glob node in the metric. It choose the most long path to reduce readings. Additional examples can be found in [tests](../finder/index_test.go).

You can overwrite automatic behavior with `index-reverse`. Valid values are `"auto", direct, "reversed"`

If you need fine tuning for different paths, you can use `[[clickhouse.index-reverses]]` to set behavior per metrics' `prefix`, `suffix` or `regexp`.

### ClickHouse aggregation
For detailed description of `max-data-points` and `internal-aggregation` see [aggregation documentation](./aggregation.md).

## Data tables `[[data-table]]`

### Rollup
The rollup configuration is used for a proper  metrics pre-aggregation. It contains two rules types:

- retention for point per time range
- aggregation function for a values

Historically, the way to define the config was `rollup-conf = "/path/to/the/conf/with/graphite_rollup.xml"`. The format is the same as [graphite_rollup](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/graphitemergetree/#rollup-configuration) scheme for ClickHouse server.

For a quite long time it's recommended to use `rollup-conf = "auto"` to get the configuration from remote ClickHouse server. It will update itself each minute.

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

`
tagged-costs = {
    "environment" = { cost: 100 },
    "project" = { values-cost = { "HugeProject" = 90 } } # overwrite tag value cost for some value only
}`

Default cost is 0 and positive or negative numbers can be used. So if environment is first tag filter in query, it will used as primary only if no other filters with equal operation. Costs from values-cost also applied to regex match or wilrdcarded equal.

## Carbonlink `[carbonlink]`
The configuration to get metrics from carbon-cache. See details in [graphite-web](https://graphite.readthedocs.io/en/latest/carbon-daemons.html#carbon-relay-py) documentation.

***TODO***: add the real use-case configuration.

## Logging `[logging]`
It's possible to set multiple loggers. See `Config` description in [config.go](https://github.com/lomik/zapwriter/blob/master/config.go) for details.

[//]: # (!!!DO NOT EDIT FOLLOWING LINES!!!)

# Example


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
 # daemon returns empty response if query matches any of regular expressions
 # target-blacklist = []
 # daemon will return the freed memory to the OS when it>0
 memory-return-interval = "0s"
 # additional request headers to log
 headers-to-log = []

[clickhouse]
 # see https://clickhouse.tech/docs/en/interfaces/http
 url = "http://localhost:8123?cancel_http_readonly_queries_on_client_close=1"
 # total timeout to fetch data
 data-timeout = "1m0s"
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
 # or how long the daemon will query tags during autocomplete
 tagged-autocomplete-days = 7

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

[carbonlink]
 server = ""
 threads-per-request = 10
 connect-timeout = "50ms"
 query-timeout = "50ms"
 # timeout for querying and parsing response
 total-timeout = "500ms"

[prometheus]
 # allows to set URL for redirect manually
 external-url = ""
 page-title = "Prometheus Time Series Collection and Processing Server"

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
