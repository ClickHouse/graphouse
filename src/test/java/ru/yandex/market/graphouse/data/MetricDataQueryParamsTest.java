package ru.yandex.market.graphouse.data;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.market.graphouse.retention.BaseRetentionProvider;
import ru.yandex.market.graphouse.retention.MetricRetention;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.InMemoryMetricDir;
import ru.yandex.market.graphouse.search.tree.MetricDir;
import ru.yandex.market.graphouse.search.tree.MetricName;

import java.util.Collections;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 16/11/2018
 */
public class MetricDataQueryParamsTest {

    private MetricName metric;

    @Before
    public void setUp() throws Exception {
        BaseRetentionProvider retentionProvider = new BaseRetentionProvider(Collections.singletonList(
            new MetricRetention.MetricDataRetentionBuilder(".*", "avg", true).addRetention(0, 60).build()
        ));
        MetricDir root = new InMemoryMetricDir(null, null, MetricStatus.SIMPLE);
        metric = new MetricName(root, "c", MetricStatus.SIMPLE, retentionProvider);
    }

    @Test
    public void test() {
        MetricDataQueryParams actualParams = MetricDataQueryParams.create(
            Collections.singletonList(metric), 1542199569, 1542200159, -1
        );
        MetricDataQueryParams expectedParams = new MetricDataQueryParams(1542199560, 1542200100, 60);
        Assert.assertEquals(expectedParams, actualParams);
    }

    @Test
    public void testLimit() {
        MetricDataQueryParams actualParams = MetricDataQueryParams.create(
            Collections.singletonList(metric), 1542199569, 1542451570, 1000
        );
        MetricDataQueryParams expectedParams = new MetricDataQueryParams(1542199500, 1542451500, 300);
        Assert.assertEquals(expectedParams, actualParams);
    }
}