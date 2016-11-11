# graphite-clickhouse
Graphite cluster backend with ClickHouse support

## Work scheme
![stack.png](doc/stack.png)

Gray components are optional or alternative

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
1. Create `/etc/graphite-clickhouse/rollup.xml` with same content as for ClickHouse. Short sample:
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
2. Create `/etc/graphite-clickhouse/graphite-clickhouse.conf`
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
3. Run
