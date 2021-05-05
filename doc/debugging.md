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

## Debug render data
All supported formats of `/render` handler are binary and may be difficult to debug. Although it's possible.

### format=pickle
To get the data in text format you may pipe the output to the following command:  
`curl 'localhost:9090/render/?format=pickle&target=metric.name&from=1619777413&until=1619778013' | python3 -c 'import pickle, sys; print(pickle.loads(sys.stdin.buffer.read()))'`

### format=protobuf (or format=carbonapi_v2_pb)
The format is relatively easy to debug. You should have `protoc` binary installed. It's usually available in `protobuf` package. Then you can run the following command from the root of the repository:  
`curl 'localhost:9090/render/?format=protobuf&target=metric.name&from=1619777413&until=1619778013' | protoc --decode carbonapi_v2_pb.MultiFetchResponse -Ivendor/ vendor/github.com/go-graphite/protocol/carbonapi_v2_pb/carbonapi_v2_pb.proto`

If the repository is not available, there's still a way to run `protoc --decode_raw`, but it's much less readable.

### format=carbonapi_v3_pb
The format is the most efficient in the meaning of network traffic and memory. At the same time it is the least debug-able. The request itself is done by sending a POST body with `carbonapi_v3_pb.MultiFetchRequest` protobuf message. So, first one have to generate the request itself, pipe it to curl, and then decode the request. To do it one should run the following command in the root of the repository:

```
echo 'metrics{startTime: 1619777413, stopTime: 1619778013, pathExpression: "metric.name"}' | \
  protoc --encode=carbonapi_v3_pb.MultiFetchRequest -Ivendor/ vendor/github.com/go-graphite/protocol/carbonapi_v3_pb/carbonapi_v3_pb.proto | \
  curl -XGET --data-binary @- 'localhost:9090/render/?format=carbonapi_v3_pb' | \
  protoc --decode=carbonapi_v3_pb.MultiFetchResponse -Ivendor/ vendor/github.com/go-graphite/protocol/carbonapi_v3_pb/carbonapi_v3_pb.proto
```

To make it a little bit easier the JSON format is implemented.

### format=json
The format exists only for debugging purpose and enabled by passing a header `X-Gch-Debug-Output: any string`. Here is a general way to debug the data:

- Optional: make a request to the frontend (carbonapi) with additional header `X-Gch-Debug-Output: a`. Then in log a similar line will be generated:  
  `INFO [render.pb3parser] v3pb_request {"request_id": "051fe964d78d9f3d33827397df779ba0", "json": "{\"metrics\":[{\"name\":\"metric.name\",\"startTime\":1619777413,\"stopTime\":1619778013,\"pathExpression\":\"metric.name\",\"maxDataPoints\":700}]}"}`
- Get the request ID from the responses request, for example: `X-Gch-Request-Id: 051fe964d78d9f3d33827397df779ba0`
- In logs either see the JSON body itself for the query ID, or look for `[render.pb3parser] pb3_target` record.
- Now to make a request just run:  
`curl -H 'Content-Type: application/json' -H 'Content-Type: application/json' -d "{\"metrics\":[{\"name\":\"metric.name\",\"startTime\":1619777413,\"stopTime\":1619778013,\"pathExpression\":\"metric.name\",\"maxDataPoints\":700}]}" 'localhost:9090/render/?format=json'`

### Marshal protobuf data with original marshallers
Both `carbonapi_v2_pb` and `carbonapi_v3_proto` have the optimized marshallers to convert ClickHouse data points to the protobuf response. But when it's necessary, it's possible to debug if the proper data is produced by passing `X-Gch-Debug-Protobuf: 1` header.
