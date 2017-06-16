Graphouse configuration
=======================

Configuration files
-------------------

Default configuration provided in `graphouse.properties` file, `/etc/graphouse` dir in deb package.
You can either edit this file, or add local-application.properties with necessary parameters.

Java VM options provided in `graphouse.vmoptions` file.

All parameters can be viewed in [graphouse-default.properties](../src/main/resources/graphouse-default.properties)


Java Options (graphouse.vmoptions)
----------------------------------
By default, Graphouse is configured with 256Mb Xms (startup memory) and 4Gb Xmx (max memory usage).
If you have a huge metric tree (>1 million metrics), it is recommended to increase Xmx.
Also it is better to set Xms equal to Xmx (in this case Graphouse will allocate all allowed memory to startup).


Metric cacher
-------------
Graphouse support [graphite plaintext protocol](http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol)

```properties
graphouse.cacher.host=localhost
graphouse.cacher.port=2003
graphouse.cacher.threads=100
graphouse.cacher.socket-timeout-millis=42000

graphouse.cacher.cache-size=2000000
graphouse.cacher.batch-size=1000000
graphouse.cacher.writers-count=2
graphouse.cacher.flush-interval-seconds=5
```


Metric validation
-----------------
Graphouse can validate incoming metrics.
You can provide validating regexp and specify min/max length or levels count using the following options:
```properties
graphouse.metric-validation.min-length=10
graphouse.metric-validation.max-length=200
graphouse.metric-validation.min-levels=2
graphouse.metric-validation.max-levels=15
graphouse.metric-validation.regexp=[-_0-9a-zA-Z\\.]*$

```