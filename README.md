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
