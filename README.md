[![deb](https://img.shields.io/badge/deb-packagecloud.io-844fec.svg)](https://packagecloud.io/go-graphite/stable)
[![rpm](https://img.shields.io/badge/rpm-packagecloud.io-844fec.svg)](https://packagecloud.io/go-graphite/stable)

# graphite-clickhouse
Graphite cluster backend with ClickHouse support

## Work scheme
![stack.png](doc/stack.png?v3)

Gray components are optional or alternative

## TL;DR
[Preconfigured docker-compose](https://github.com/lomik/graphite-clickhouse-tldr)

### Docker
Docker images are available on [packages](https://github.com/lomik/graphite-clickhouse/pkgs/container/graphite-clickhouse) page.

## Compatibility
- [x] [graphite-web 1.1.0](https://github.com/graphite-project/graphite-web)
- [x] [graphite-web 0.9.15](https://github.com/graphite-project/graphite-web/tree/0.9.15)
- [x] [graphite-web 1.0.0](https://github.com/graphite-project/graphite-web)
- [x] [carbonapi 0.14.1+](https://github.com/go-graphite/carbonapi)
- [x] [carbonzipper](https://github.com/go-graphite/carbonzipper) (DEPRECATED, is part of carbonapi currently)

## Build
Required golang 1.15+
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
See [configuration documentation](./doc/config.md).

### Special headers processing

Some HTTP headers are processed specially by the service

#### Request headers

*Grafana headers*: `X-Dashboard-Id`, `X-Grafana-Org-Id`, and `X-Panel-Id` are logged and passed further to the ClickHouse.

*Debug headers* (see [debugging.md](./doc/debugging.md) for details):

- `X-Gch-Debug-External-Data` - when this header is set to anything and every of `directory`, `directory-perm`, and `external-data-perm` parameters in `[debug]` is set and valid, service will save the dump of external data tables in the directory for debug output.
- `X-Gch-Debug-Output` - header to enable special processing for `format=carbonapi_v3_pb` and `format=json` render output.
- `X-Gch-Debug-Protobuf` - header enables the original marshallers for `protobuf` and `carbonapi_v3_pb` to check the binary data integrity.

#### Response headers

- `X-Gch-Request-Id` - the current request ID.
- `X-Cached-Find`    - Flag for find cache hit.

## Run on same host with old graphite-web 0.9.x
By default graphite-web won't connect to CLUSTER_SERVER on localhost. Cheat:
```python
class ForceLocal(str):
    def split(self, *args, **kwargs):
        return ["8.8.8.8", "8080"]

CLUSTER_SERVERS = [ForceLocal("127.0.0.1:9090")]
```
