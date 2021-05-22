# Enable ClickHouse aggregation

The feature was added in [v0.12.0](https://github.com/lomik/graphite-clickhouse/releases/tag/v0.12.0). It's enabled by default since [v0.13.0](https://github.com/lomik/graphite-clickhouse/releases/tag/v0.13.0) ([#157](https://github.com/lomik/graphite-clickhouse/pull/157)). You can disable it be setting `internal-aggregation = false` to use aggregation in graphite-clickhouse.

```
[clickhouse]
# ClickHouse-side aggregation
internal-aggregation = true
# maximum number of points per metric. It should be set to 4096 or less for ClickHouse older than 20.8
# https://github.com/ClickHouse/ClickHouse/commit/d7871f3976e4f066c6efb419db22725f476fd9fa
max-data-points = 1048576
```

The only known _frontend_ supporting passing `maxDataPoints` from requests is [carbonapi>=0.14](https://github.com/go-graphite/carbonapi/releases/tag/0.14.0). Protocol should be set to `carbonapi_v3_pb` for this feature to fully work, see [config](https://github.com/go-graphite/carbonapi/blob/main/doc/configuration.md#upstreams)->backendv2->backends->protocol.

But even without mentioned adjustments, `internal-aggregation` improves the whole picture by implementing whisper-like aggregation behavior (see below).

## Compatible ClickHouse versions
The feature uses ClickHouse aggregation combinator [-Resample](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-resample). This aggregator is available since version [19.11](https://github.com/ClickHouse/ClickHouse/commit/57db1fac5990a7227e720c9dd438d88a381d298f)

*Note*: version 0.12 is compatible only with CH 20.1.13.105, 20.3.10.75, 20.4.5.36, 20.5.2.7 or newer since it uses -OrNull modifier.

Generally, it's a good idea to always use the latest [LTS](https://repo.clickhouse.tech/deb/lts/main/) ClickHouse release to have the actual version.

## Upgrade

- Upgrade the carbonapi to version 0.14.0 or greater
- Upgrade graphite-clickhouse to version 0.12.0 or greater
- Set the `backendv2->backends->protocol: carbonapi_v3_pb` in carbonapi *only after graphite-clickhouse is upgraded*
- Upgrade ClickHouse
- Enable `internal-aggregation` in graphite-clickhouse

# Historical remark: schemes and changes overview
## Classic whisper scheme

```
header:
xFilesFactor: [0, 1]
aggregation: {avg,sum,min,max,...}
retention: 1d:1m,1w:5m,1y:1h
data:
archive1: 1440 points
archive2: 2016 points
archive3: 8760 points
```

- Retention description:
  - Stores point/1m for one day
  - Stores point/5m for one week
  - Stores point/1h for one year
  - Older points than any of mentioned age are overwriten by new incoming points
- Each archive filled up simultaneously
- Aggregation on the fly during writing
- `xFilesFactor` controls if points from archive(N) should be aggregated into archive(N+1)
- Points are selected only from one archive, with the most precision:
  - from <= now-1d -> archive1
  - from <= now-7d -> archive2
  - else -> archive3

## Historical graphite-clickhouse way

### Storing: GraphiteMergeTree table engine in ClickHouse (CH)

Completely another principle of data storage.

- Retention scheme looks slightly different:  
`retention: 0:60,1d:5m,1w:1h,1y:1d`
  - Stores point/minute, if the age of point is at least 0sec
  - Stores point/5min, if the age of point is at least one day
  - Stores point/1h, if the age of point is at least one week
  - GraphiteMergeTree doesn't drop metrics after some particular age, so after one year we would store it with the minimum possible resolution point/day
- Retention and aggregation policies are applied only when point becomes older than X (1d,1w,1y)
- There is no such thing as `archive`, each point is stored only once
- No `xFilesFactor` entity: each point will be aggregated

### Fetching data: before September 2019 (current `internal-aggregation = false` behavior)

```sql
SELECT Path, Time, Value, Timestamp
FROM data WHERE ...
```

Logic:

- Select all points
- Aggregate them on the fly to the proper `archive` step
- Pass further to *graphite-web*/*carbonapi*

Problems:

- A huge overhead for Path (the heaviest part)
- Extremely inefficient in terms of network traffic, especially when CH cluster is used
  - The CH node `query-initiator` must collect the whole data (in memory or on the disk), and only then points will be passed further

### Fetching data: after September 2019 ([#61](https://github.com/lomik/graphite-clickhouse/pull/61), [#62](https://github.com/lomik/graphite-clickhouse/pull/62), [#65](https://github.com/lomik/graphite-clickhouse/pull/65)) (between v0.11.7 and v0.12.0)

```sql
SELECT Path,
  groupArray(Time),
  groupArray(Value),
  groupArray(Timestamp)
FROM data WHERE ... GROUP BY Path
```

- Up to 6 time less network load
- But still selects all points and aggregates in *graphite-clickhouse*

### Fetching data: September 2020 ([#88](https://github.com/lomik/graphite-clickhouse/pull/88)) (v0.12.0)

```sql
SELECT Path,
  arrayFilter(x->isNotNull(x),
    anyOrNullResample($from, $until, $step)
      (toUInt32(intDiv(Time, $step)*$step), Time)
  ),
  arrayFilter(x->isNotNull(x),
    ${func}OrNullResample($from, $until, $step)
      (Value, Time)
  )
FROM data WHERE ... GROUP BY Path
```

- This solution implements `archive` analog on CH side
- Most of the data is aggregated on CH shards and doesn't leave them, so `query-initiator` consumes much less memory
- When *carbonapi* with `format=carbonapi_v3_pb` is used, the `/render?maxDataPoints=x` parameter processed on CH side too

### Fetching data: April 2021 ([#145](https://github.com/lomik/graphite-clickhouse/pull/145))

```sql
WITH anyResample($from, $until, $step)(toUInt32(intDiv(Time, $step)*$step), Time) AS mask
SELECT Path,
 arrayFilter(m->m!=0, mask) AS times,
 arrayFilter((v,m)->m!=0, ${func}Resample($from, $until, $step)(Value, Time), mask) AS values
FROM data WHERE ... GROUP BY Path
```

- Query improved a bit: dropped the use of `-OrNull` improved compatibility with different CH versions.

## Fetching data: concepts' difference

For small requests, the difference is not so big, but for the heavy one the amount of data was decreased up to 100 times:

```
target=${986_metrics_60s_precision}
from=-7d
maxDataPoints=100
```

| method     | rows    | points  | data (binary)    | time (s) |
| -          | -       | -       | -                | -        |
| row/point  | 9887027 | 9887027 | 556378258 (530M) | 16.486   |
| groupArray | 986     | 9887027 | 158180388 (150M) | 35.498   |
| -Resample  | 986     | 98553   | 1421418 (1M)     | 13.181   |

*note*: it's localhost, so with slow network effect may be even more significant.

### The maxDataPoints processing

The classical pipeline:

- Fetch the data in *graphite-web*/*carbonapi*
- Apply all functions from `target`
- Compare the result with `maxDataPoints` URI parameter and adjust them

Current:

- Get data, aggregated with the proper function directly from CH
- Fetch pre-aggregated data with a proper functions from ClickHouse
- Apply all functions to the pre-aggregated data

