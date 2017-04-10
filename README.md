graphouse
=========

Graphouse allows you to use ClickHouse as a Graphite storage.

[Installation guide](doc/install.md)

[Configuration](doc/config.md)

[Build](doc/build.md)

Overview
--------
Graphouse includes:
- Tpc server to receive metrics with [Graphite plaintext protocol](http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol).
- Http api for metric search and data retrieval (with graphite-web python module).
- Http api for metric tree management.

Comparing Graphouse with [common Graphite architecture](https://github.com/graphite-project/graphite-web#overview).


![arch_overview](doc/img/arch_overview.png)