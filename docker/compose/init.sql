CREATE DATABASE graphite;

CREATE TABLE graphite.metrics (
    date Date DEFAULT toDate(0),
    name String,
    level UInt16,
    parent String,
    updated DateTime DEFAULT now(),
    status Enum8('SIMPLE' = 0, 'BAN' = 1, 'APPROVED' = 2, 'HIDDEN' = 3, 'AUTO_HIDDEN' = 4)
)
ENGINE = ReplacingMergeTree(updated)
PARTITION BY toYYYYMM(date)
ORDER BY (parent, name)
SETTINGS index_granularity = 1024;

CREATE TABLE graphite.data (
    metric String,
    value Float64,
    timestamp UInt32,
    date Date,  updated UInt32
)
ENGINE = GraphiteMergeTree('graphite_rollup')
PARTITION BY toYYYYMM(date)
ORDER BY (metric, timestamp)
SETTINGS index_granularity = 8192;