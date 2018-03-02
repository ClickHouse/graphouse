ATTACH TABLE metrics
(
    date Date DEFAULT toDate(0), 
    name String, 
    level UInt16, 
    parent String, 
    updated DateTime DEFAULT now(), 
    status Enum8('SIMPLE' = 0, 'BAN' = 1, 'APPROVED' = 2, 'HIDDEN' = 3, 'AUTO_HIDDEN' = 4)
) ENGINE = ReplacingMergeTree(date, (parent, name), 1024, updated)
