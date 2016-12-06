# graphite-clickhouse
Graphite cluster backend with ClickHouse support

## Work scheme
![stack.png](doc/stack.png?v2)

Gray components are optional or alternative

## Compatibility
- [x] [graphite-web 0.9.15](https://github.com/graphite-project/graphite-web/tree/0.9.15)
- [ ] [graphite-web master](https://github.com/graphite-project/graphite-web): unknown
- [ ] [carbonzipper](https://github.com/dgryski/carbonzipper): supported, untested
- [ ] [carbonapi](https://github.com/dgryski/carbonapi): unknown

## Build
Required golang 1.7+
```sh
# build binary
git clone https://github.com/lomik/graphite-clickhouse.git
cd graphite-clickhouse
make submodules
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

For complex clickhouse queries you might need to increase default query_max_size. To do that add following line to `/etc/clickhouse-server/users.xml` for the user you are using:
```xml
<!-- Default is 262144 -->
<max_query_size>10485760</max_query_size>
```

Create `/etc/graphite-clickhouse/graphite-clickhouse.conf`
```toml
[common]
listen = ":9090"
logfile = "/var/log/graphite-clickhouse/graphite-clickhouse.log"
loglevel = "info"
max-cpu = 1

[clickhouse]
# You can add user/password (http://user:password@localhost:8123) and any clickhouse options (GET-parameters) to url
# It is recommended to create read-only user 
url = "http://localhost:8123"
data-table = "graphite"
tree-table = "graphite_tree"
rollup-conf = "/etc/graphite-clickhouse/rollup.xml"
# Add extra prefix (directory in graphite) for all metrics
extra-prefix = ""
data-timeout = "1m0s"
tree-timeout = "1m0s"
```

## Run on same host with graphite-web
By default graphite-web won't connect to CLUSTER_SERVER on localhost. Cheat:
```python
class ForceLocal(str):
    def split(self, *args, **kwargs):
        return ["8.8.8.8", "8080"]

CLUSTER_SERVERS = [ForceLocal("127.0.0.1:9090")]
```
