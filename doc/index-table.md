# Index table
The `index` type table is used to look up metrics that [match the query](https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards).

```sql
CREATE TABLE graphite_index (
  Date Date,
  Level UInt32,
  Path String,
  Version UInt32
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(Date)
ORDER BY (Level, Path, Date);
```

Where:
* `Date` - date from received point. Or constant date for full metric list (`1970-02-12` (`toDate(42)`) by default)
* `Level` - metrics depth, see description below
* `Version` - unix timestamp when the last point was received, only the last one is stored in ReplacingMergeTree table engine

Each metric creates multiple entries in a table:
* daily with direct Path and plain level
* daily with reversed Path and Level = 10000+OriginalLevel
* records with constant Date and Level = 20000+OriginalLevel for metric itself and all it parents
* record with constant Date, reversed Path and Level = 30000+OriginalLevel

For example, getting the metric `lorem.ipsum.dolor.sit.amet` adds the following entries to the table:

| Date          | Level | Path                       | Version    |
| ------------- | ------| -------------------------- | ---------- |
| 2019-05-14    | 5     | lorem.ipsum.dolor.sit.amet | 1557827619 |
| 2019-05-14    | 10005 | amet.sit.dolor.ipsum.lorem | 1557827619 |
| 1970-02-12    | 20001 | lorem.                     | 1557827619 |
| 1970-02-12    | 20002 | lorem.ipsum.               | 1557827619 |
| 1970-02-12    | 20003 | lorem.ipsum.dolor.         | 1557827619 |
| 1970-02-12    | 20004 | lorem.ipsum.dolor.sit.     | 1557827619 |
| 1970-02-12    | 20005 | lorem.ipsum.dolor.sit.amet | 1557827619 |
| 1970-02-12    | 30005 | amet.sit.dolor.ipsum.lorem | 1557827619 |

If you'd like to use only fixed date for index, `index-use-daily = false` can be set in `[clickhouse]` configuration. To prevent continuous growing up of index table, parameter `disable-daily-index = false` should be set in carbon-clickhouse.

### Migrate `tree` table

```sql
-- direct Path and parents
INSERT INTO graphite_index (Date, Level, Path, Version)
SELECT
  '1970-02-12',
  Level+20000,
  Path,
  Version
FROM graphite_tree;

-- reversed Path without parents
INSERT INTO graphite_index (Date, Level, Path, Version)
SELECT
  '1970-02-12',
  Level+30000,
  arrayStringConcat(arrayMap(x->reverse(x), splitByChar('.', reverse(Path))), '.'),
  Version
FROM graphite_tree
WHERE NOT Path LIKE '%.';
```

### Migrate `series` table
```sql
-- direct Path
INSERT INTO graphite_index (Date, Level, Path, Version)
SELECT
  Date,
  Level,
  Path,
  Version
FROM graphite_series;

-- reverse Path
INSERT INTO graphite_index (Date, Level, Path, Version)
SELECT
  Date,
  Level+10000,
  arrayStringConcat(arrayMap(x->reverse(x), splitByChar('.', reverse(Path))), '.'),
  Version
FROM graphite_series;
```

### Migrate `series-reverse` table
```sql
-- direct Path
INSERT INTO graphite_index (Date, Level, Path, Version)
SELECT
  Date,
  Level,
  arrayStringConcat(arrayMap(x->reverse(x), splitByChar('.', reverse(Path))), '.'),
  Version
FROM graphite_series_reverse;

-- reverse Path
INSERT INTO graphite_index (Date, Level, Path, Version)
SELECT
  Date,
  Level+10000,
  Path,
  Version
FROM graphite_series_reverse;
```
