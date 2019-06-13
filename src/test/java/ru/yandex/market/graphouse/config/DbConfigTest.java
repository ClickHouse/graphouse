package ru.yandex.market.graphouse.config;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.util.Arrays;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/03/2018
 */
public class DbConfigTest {

    @Test
    public void clickHouseSimpleDataSource() {
        DbConfig dbConfig = new DbConfig();
        DataSource ds = dbConfig.clickHouseDataSource("host1", 42, "db", -1, new ClickHouseProperties(), null, null);
        Assert.assertTrue(ds instanceof ClickHouseDataSource);
        ClickHouseDataSource cds = (ClickHouseDataSource) ds;
        Assert.assertEquals("host1", cds.getHost());
        Assert.assertEquals("db", cds.getDatabase());
        Assert.assertEquals(42, cds.getPort());
    }

    @Test
    public void clickHouseBalancedDataSource() {
        DbConfig dbConfig = new DbConfig();
        DataSource ds = dbConfig.clickHouseDataSource("host1,host2", 42, "db", -1, new ClickHouseProperties(), null, null);
        Assert.assertTrue(ds instanceof BalancedClickhouseDataSource);
        BalancedClickhouseDataSource cds = (BalancedClickhouseDataSource) ds;
        Assert.assertEquals(Arrays.asList("jdbc:clickhouse://host1:42/db", "jdbc:clickhouse://host2:42/db"), cds.getAllClickhouseUrls());
    }
}