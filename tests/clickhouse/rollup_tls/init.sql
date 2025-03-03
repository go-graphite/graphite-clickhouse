CREATE TABLE IF NOT EXISTS default.graphite_reverse (
  Path String,
  Value Float64,
  Time UInt32,
  Date Date,
  Timestamp UInt32
) ENGINE = GraphiteMergeTree('graphite_rollup')
PARTITION BY toYYYYMM(Date)
ORDER BY (Path, Time);

CREATE TABLE IF NOT EXISTS default.graphite (
  Path String,
  Value Float64,
  Time UInt32,
  Date Date,
  Timestamp UInt32
) ENGINE = GraphiteMergeTree('graphite_rollup')
PARTITION BY toYYYYMM(Date)
ORDER BY (Path, Time);

CREATE TABLE IF NOT EXISTS default.graphite_index (
  Date Date,
  Level UInt32,
  Path String,
  Version UInt32
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(Date)
ORDER BY (Level, Path, Date);

CREATE TABLE IF NOT EXISTS default.graphite_tags (
  Date Date,
  Tag1 String,
  Path String,
  Tags Array(String),
  Version UInt32
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(Date)
ORDER BY (Tag1, Path, Date);

CREATE TABLE IF NOT EXISTS default.tag1_count_per_day
(
  Date Date,
  Tag1 String,
  Count UInt64
)
ENGINE = SummingMergeTree
ORDER BY (Date, Tag1);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.tag1_count_per_day_mv TO default.tag1_count_per_day AS
SELECT Date AS Date,
       Tag1 AS Tag1,
       count(*) AS Count
FROM default.graphite_tags
GROUP BY (Date, Tag1);