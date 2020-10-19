[![deb](https://img.shields.io/badge/deb-packagecloud.io-844fec.svg)](https://packagecloud.io/go-graphite/stable)
[![rpm](https://img.shields.io/badge/rpm-packagecloud.io-844fec.svg)](https://packagecloud.io/go-graphite/stable)

# graphite-clickhouse
Graphite cluster backend with ClickHouse support

## Work scheme
![stack.png](doc/stack.png?v3)

Gray components are optional or alternative

## TL;DR
[Preconfigured docker-compose](https://github.com/lomik/graphite-clickhouse-tldr)

## Compatibility
- [x] [graphite-web 1.1.0](https://github.com/graphite-project/graphite-web)
- [x] [graphite-web 0.9.15](https://github.com/graphite-project/graphite-web/tree/0.9.15)
- [x] [graphite-web 1.0.0](https://github.com/graphite-project/graphite-web)
- [x] [carbonzipper](https://github.com/go-graphite/carbonzipper)
- [x] [carbonapi](https://github.com/go-graphite/carbonapi)

## Build
Required golang 1.13+
```sh
# build binary
git clone https://github.com/lomik/graphite-clickhouse.git
cd graphite-clickhouse
make
```

## Installation
1. Setup [Yandex ClickHouse](https://github.com/yandex/ClickHouse) and [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)
2. Setup and configure `graphite-clickhouse`
3. Add graphite-clickhouse `host:port` to graphite-web [CLUSTER_SERVERS](http://graphite.readthedocs.io/en/latest/config-local-settings.html#cluster-configuration)

## Configuration
Create `/etc/graphite-clickhouse/rollup.xml` with same content as for ClickHouse. Short sample:
```xml
<graphite_rollup>
        <default>
                <function>avg</function>
                <retention>
                        <age>0</age>
                        <precision>60</precision>
                </retention>
                <retention>
                        <age>2592000</age>
                        <precision>3600</precision>
                </retention>
        </default>
</graphite_rollup>
```

For complex ClickHouse queries you might need to increase default query_max_size. To do that add following line to `/etc/clickhouse-server/users.xml` for the user you are using:
```xml
<!-- Default is 262144 -->
<max_query_size>10485760</max_query_size>
```

Create `/etc/graphite-clickhouse/graphite-clickhouse.conf`
```toml
[common]
listen = ":9090"
# Listener to serve /debug/pprof requests. `-pprof` argument would override it
pprof-listen = ""
max-cpu = 1
# How frequently to call debug.FreeOSMemory() to return memory back to OS
# Setting it to zero disables this functionality
memory-return-interval = "0s"
# Limit number of results from find query. Zero = unlimited
max-metrics-in-find-answer = 0
# Limit numbers of queried metrics per target in /render requests. Zero = unlimited
max-metrics-per-target = 15000
# Daemon returns empty response if query matches any of regular expressions
# target-blacklist = ["^not_found.*"]
# If this > 0, then once an interval daemon will return the freed memory to the OS
memory-return-interval = "0s"

[clickhouse]
# You can add user/password (http://user:password@localhost:8123) and any clickhouse options (GET-parameters) to url
# It is recommended to create read-only user 
url = "http://localhost:8123"
# Add extra prefix (directory in graphite) for all metrics
extra-prefix = ""

# Default table with points
data-table = "graphite"
data-timeout = "1m0s"
# Rollup rules xml filename. Use `auto` magic word for select rollup rules from ClickHouse
rollup-conf = "/etc/graphite-clickhouse/rollup.xml"

# Table with series list (daily and full)
# https://github.com/lomik/graphite-clickhouse/wiki/IndexTable
index-table = "graphite_index"
# Use daily data from index table. This is useful for installations with big count of short-lived series but can be slower in other cases
index-use-daily = true
index-timeout = "1m"

# `tagged` table from carbon-clickhouse. Required for seriesByTag
tagged-table = ""
# For how long the daeom will query tags during autocomplete
tagged-autocomplete-days = 7

# Old index tables. DEPRECATED
tree-table = "graphite_tree"
# Optional table with daily series list.
# Useful for installations with big count of short-lived series
date-tree-table = ""
# Supported several schemas of date-tree-table:
# 1 (default): table only with Path, Date, Level fields. Described here: https://habrahabr.ru/company/avito/blog/343928/
# 2: table with Path, Date, Level, Deleted, Version fields. Table type "series" in the carbon-clickhouse
# 3: same as #2 but with reversed Path. Table type "series-reverse" in the carbon-clickhouse
date-tree-table-version = 0
tree-timeout = "1m0s"

connect-timeout = "1s"

# Sets the maximum for maxDataPoints parameter.
# If you use CH w/o https://github.com/ClickHouse/ClickHouse/pull/13947, you have to set it to 4096
max-data-points = 4096
# Use metrics aggregation on ClickHouse site.
# This feature is very useful, read https://github.com/lomik/graphite-clickhouse/wiki/ClickHouse-aggregation-VS-graphite%E2%80%94clickhouse-aggregation
internal-aggregation = false

[prometheus]
# The URL under which Prometheus is externally reachable (for example, if Prometheus is served via a reverse proxy). Used for
# generating relative and absolute links back to Prometheus itself. If the URL has a path portion, it will be used to prefix all
# HTTP endpoints served by Prometheus. If omitted, relevant URL components will be derived automatically.
external-url = ""
page-title = "Prometheus Time Series Collection and Processing Server"

[carbonlink]
server = ""
threads-per-request = 10
connect-timeout = "50ms"
query-timeout = "50ms"
total-timeout = "500ms"

# You can define multiple data tables (with points).
# The first table that matches is used.
#
# # Sample, archived table with points older 30d
# [[data-table]]
# table = "graphite_archive"
# min-age = "720h"
# 
# # All available options
# [[data-table]]
# # clickhouse table name
# table = "table_name"
# # points in table are stored with reverse path
# reverse = false
# # Custom rollup.xml for table. 
# # Magic word `auto` can be used for load rules from ClickHouse
# # With value `none` only rollup-default-precision and rollup-default-function will be used for rollup
# rollup-conf = ""
# # Which table to discover rollup-rules from. If not specified - will use what specified in "table" parameter.
# # Useful when reading from distributed table, but the rollup parameters are on the shard tables.
# # Can be in "database.table" form.
# rollup-auto-table = ""
# # Sets the default precision and function for rollup patterns which don't have age=0 retention defined.
# # If age=0 retention is defined in the rollup config then it takes precedence.
# # If left at the default value of 0 then no rollup is performed when the requested interval 
# # is not covered by any rollup rule. In this case the points will be served with 60 second precision.
# rollup-default-precision = 60
# rollup-default-function = "avg"
# # from >= now - {max-age}
# max-age = "240h"
# # until <= now - {min-age}
# min-age = "240h"
# # until - from <= {max-interval}
# max-interval = "24h"
# # until - from >= {min-interval}
# min-interval = "24h"
# # regexp.Match({target-match-any}, target[0]) || regexp.Match({target-match-any}, target[1]) || ...
# target-match-any = "regexp"
# # regexp.Match({target-match-all}, target[0]) && regexp.Match({target-match-all}, target[1]) && ...
# target-match-all = "regexp"

[debug]
# The directory for debug info. If set, additional info may be saved there
directory = "/var/log/graphite-clickhouse/debug"
directory-perm = "0755"
# File permissions for external data dumps. Enabled only if !=0, see X-Gch-Debug-External-Data header
# Format is octal, e.g. 0640
external-data-perm = "0644"

[[logging]]
logger = ""
file = "/var/log/graphite-clickhouse/graphite-clickhouse.log"
level = "info"
encoding = "mixed"
encoding-time = "iso8601"
encoding-duration = "seconds"
```

### Special headers processing

Some HTTP headers are processed specially by the service

#### Request headers

*Grafana headers*: `X-Dashboard-Id`, `X-Grafana-Org-Id`, and `X-Panel-Id` are logged and passed further to the ClickHouse.

*Debug headers*:

- `X-Gch-Debug-External-Data` - when this header is set to anything and every of `directory`, `directory-perm`, and `external-data-perm` parameters in `[debug]` is set and valid, service will save the dump of external data tables in the directory for debug output.

#### Response headers

- `X-Gch-Request-Id` - the current request ID.

## Run on same host with old graphite-web 0.9.x
By default graphite-web won't connect to CLUSTER_SERVER on localhost. Cheat:
```python
class ForceLocal(str):
    def split(self, *args, **kwargs):
        return ["8.8.8.8", "8080"]

CLUSTER_SERVERS = [ForceLocal("127.0.0.1:9090")]
```
