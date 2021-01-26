# Debug graphite-clickhouse
## General config
The `debug` section contains common parameters:

```toml
[debug]
directory = '/var/log/graphite-clickhouse/debug'  # where the additional debug information will be dumped.
directory-perm = '0644'  # file mode for the directory. It's applied only if directory does not exist.
```

## Debug queries with external data
All queries to the `data-table` tables use external data. It reduces the SQL parsing time and allows to query big number of metrics without generating 100k+ characters SQL query.

Unfortunately, it requires some additional effort to reproduce the query in case of problems.

In PR [#126](https://github.com/lomik/graphite-clickhouse/pull/126) it's solved. All you need to do is set the additional config parameter in `[debug]` (see `General config` above):

```toml
[debug]
external-data-perm = '0640'  # to not read the metrics by anybody
```

And pass the HTTP header `X-Gch-Debug-External-Data` with any value in the `/render` or Prometheus request. It will produce the external data dump files in the debug directory and generate a `curl` command in the log on INFO level.

E.g. `[2021-01-26T09:57:33.548+0100] INFO [render] external-data {"request_id": "7994db164f6eef7f2e4da20c54c089f2", "debug command": "curl -F 'metrics_list=@/tmp/ext-data-debug/ext-metrics_list:7994db164f6eef7f2e4da20c54c089f2.TSV;' 'http://graphite:xxxxx@clickhouse-hostname.tld:8123/?cancel_http_readonly_queries_on_client_close=1&metrics_list_format=TSV&metrics_list_structure=Path+String&query=SELECT+Path%2C%0A%09arrayFilter%28x-%3EisNotNull%28x%29%2C+anyOrNullResample%281611590400%2C+1611594059%2C+60%29%28toUInt32%28intDiv%28Time%2C+60%29%2A60%29%2C+Time%29%29%2C%0A%09arrayFilter%28x-%3EisNotNull%28x%29%2C+avgOrNullResample%281611590400%2C+1611594059%2C+60%29%28Value%2C+Time%29%29%0AFROM+graphite.data%0APREWHERE+Date+%3E%3D%272021-01-25%27+AND+Date+%3C%3D+%272021-01-25%27%0AWHERE+%28Path+in+metrics_list%29+AND+%28Time+%3E%3D+1611590400+AND+Time+%3C%3D+1611594059%29%0AGROUP+BY+Path%0AFORMAT+RowBinary&query_id=7994db164f6eef7f2e4da20c54c089f2%3Adebug'"}`

If URL contains user and password, it will be redacted to not expose the credentials.
