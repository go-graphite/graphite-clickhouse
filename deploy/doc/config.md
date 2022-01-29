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

