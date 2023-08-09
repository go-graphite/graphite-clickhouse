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
