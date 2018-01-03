ATTACH TABLE data
(
    metric String, 
    value Float64, 
    timestamp UInt32, 
    date Date, 
    updated UInt32
) ENGINE = ReplacingMergeTree(date, (metric, timestamp), 8192, updated)
