Building cluster
================

Replication
-----------

- Configure clickhouse, see [doc](https://clickhouse.yandex/reference_en.html#Data_replication) for more details.
- Create Replicated tables

```sql
CREATE DATABASE graphite;

CREATE TABLE graphite.metrics ( date Date DEFAULT toDate(0),  name String,  level UInt16,  parent String,  updated DateTime DEFAULT now(),  status Enum8('SIMPLE' = 0, 'BAN' = 1, 'APPROVED' = 2, 'HIDDEN' = 3, 'AUTO_HIDDEN' = 4)) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/graphite.metrics', '{replica}', date, (level, parent, name), 1024, updated);

CREATE TABLE graphite.data ( metric String,  value Float64,  timestamp UInt32,  date Date,  updated UInt32) ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/graphite.data', '{replica}',date, (metric, timestamp), 8192, 'graphite_rollup');
```

Sharding and Replication
------------------------

- Configure clickhouse, see [replicating](https://clickhouse.yandex/reference_en.html#Data_replication) and [sharding](https://clickhouse.yandex/reference_en.html#Distributed) docs for more details.
- Create tables
```sql
CREATE DATABASE graphite;

CREATE TABLE graphite.metrics ( date Date DEFAULT toDate(0),  name String,  level UInt16,  parent String,  updated DateTime DEFAULT now(),  status Enum8('SIMPLE' = 0, 'BAN' = 1, 'APPROVED' = 2, 'HIDDEN' = 3, 'AUTO_HIDDEN' = 4)) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/single/graphite.metrics', '{replica}', date, (level, parent, name), 1024, updated);

CREATE TABLE graphite.data_lr ( metric String,  value Float64,  timestamp UInt32,  date Date,  updated UInt32) ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/{shard}/graphite.data_lr', '{replica}', date, (metric, timestamp), 8192, 'graphite_rollup')

CREATE TABLE graphite.data ( metric String,  value Float64,  timestamp UInt32,  date Date,  updated UInt32) ENGINE Distributed(CLICKHOUSE_CLUSTER_NAME, 'graphite', 'data_lr', sipHash64(metric))
```

Don't forget to replace ```CLICKHOUSE_CLUSTER_NAME``` with name from you config.

**Notice**: We use sharding only for ```data```, couse ```metrics``` is small and contains only metric names.