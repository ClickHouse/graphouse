Installation guide
==================

ClickHouse
----------

- [Install ClickHouse.](https://clickhouse.yandex/docs/en/getting_started.html#installation)

- Create rollup config `/etc/clickhouse-server/conf.d/graphite_rollup.xml`.
Pay attention to **graphite_rollup** tag name. The name is used below.

```xml
<yandex>
<graphite_rollup>
    <path_column_name>metric</path_column_name>
    <time_column_name>timestamp</time_column_name>
    <value_column_name>value</value_column_name>
    <version_column_name>updated</version_column_name>
	<pattern>
		<regexp>^five_sec</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>5</precision>
		</retention>
		<retention>
			<age>2592000</age>
			<precision>60</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>600</precision>
		</retention>
	</pattern>

	<pattern>
		<regexp>^one_min</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>60</precision>
		</retention>
		<retention>
			<age>2592000</age>
			<precision>300</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>1800</precision>
		</retention>
	</pattern>

	<pattern>
		<regexp>^five_min</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>300</precision>
		</retention>
		<retention>
			<age>2592000</age>
			<precision>600</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>1800</precision>
		</retention>
	</pattern>

	<pattern>
		<regexp>^one_sec</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>1</precision>
		</retention>
		<retention>
			<age>2592000</age>
			<precision>60</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>300</precision>
		</retention>
	</pattern>

	<pattern>
		<regexp>^one_hour</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>3600</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>86400</precision>
		</retention>
	</pattern>

	<pattern>
		<regexp>^ten_min</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>600</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>3600</precision>
		</retention>
	</pattern>

	<pattern>
		<regexp>^one_day</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>86400</precision>
		</retention>
	</pattern>

	<pattern>
		<regexp>^half_hour</regexp>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>1800</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>3600</precision>
		</retention>
	</pattern>

	<default>
		<function>any</function>
		<retention>
			<age>0</age>
			<precision>60</precision>
		</retention>
		<retention>
			<age>2592000</age>
			<precision>300</precision>
		</retention>
		<retention>
			<age>31104000</age>
			<precision>1800</precision>
		</retention>
	</default>
</graphite_rollup>
</yandex>
```
- Create tables
```sql
CREATE DATABASE graphite;

CREATE TABLE graphite.metrics ( date Date DEFAULT toDate(0),  name String,  level UInt16,  parent String,  updated DateTime DEFAULT now(),  status Enum8('SIMPLE' = 0, 'BAN' = 1, 'APPROVED' = 2, 'HIDDEN' = 3, 'AUTO_HIDDEN' = 4)) ENGINE = ReplacingMergeTree(date, (parent, name), 1024, updated);

CREATE TABLE graphite.data ( metric String,  value Float64,  timestamp UInt32,  date Date,  updated UInt32) ENGINE = GraphiteMergeTree(date, (metric, timestamp), 8192, 'graphite_rollup');
```

**Notice**: If you don't want ClickHouse to rollup data, you can use ReplacingMergeTree instead of GraphiteMergeTree.
```sql
CREATE TABLE graphite.data ( metric String,  value Float64,  timestamp UInt32,  date Date,  updated UInt32) ENGINE = ReplacingMergeTree(date, (metric, timestamp), 8192, updated)
```
But you still need to describe the rules for the rotation, so that Graphouse knows its metrics retention.


Graphouse
---------
- Add ClickHouse debian repo. [See doc.](https://clickhouse.yandex/docs/en/getting_started.html#installation)
- [Install JDK8.](https://tecadmin.net/install-oracle-java-8-ubuntu-via-ppa/)
- Install Graphouse `sudo agt-get install graphouse`
- Set `graphouse.clickhouse.retention-config` property in graphouse config /etc/graphouse/graphouse.properties. You can skip this step, then [default config](../src/main/java/ru/yandex/market/graphouse/retention/DefaultRetentionProvider.java#L29) will be used
- Start graphouse `sudo /etc/init.d/graphouse start`

If you have any problems check graphouse log dir for details `/var/log/graphouse`.
See [Configuration](config.md) for more details.


Graphite-web
------------
- Install [graphite-web](http://graphite.readthedocs.io/en/latest/), if you don't have it already. You don't need carbon or whisper, Graphouse and ClickHouse completely replace them.
- Add graphouse plugin `/opt/graphouse/bin/graphouse.py` to your graphite webapp root dir.
For example, if you dir is `/opt/graphite/webapp/graphite/` use command below
```bash
sudo ln -fs /opt/graphouse/bin/graphouse.py /opt/graphite/webapp/graphite/graphouse.py
```

- Configure storage finder in your [local_settings.py](http://graphite.readthedocs.io/en/latest/config-local-settings.html)
```python
STORAGE_FINDERS = (
    'graphite.graphouse.GraphouseFinder',
)
```
- Restart graphite-web


