# graphite-clickhouse
Graphite cluster backend with ClickHouse support

## Work scheme
![stack.png](doc/stack.png)

Gray components are optional or alternative for default

## Build
Required golang 1.7+
```sh
# build binary
git clone https://github.com/lomik/graphite-clickhouse.git
cd graphite-clickhouse
make submodules
make
```
