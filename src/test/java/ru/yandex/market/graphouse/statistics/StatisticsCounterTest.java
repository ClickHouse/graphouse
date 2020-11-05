package ru.yandex.market.graphouse.statistics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 26.12.17
 */
@RunWith(MockitoJUnitRunner.class)
public class StatisticsCounterTest {
    @Captor
    private ArgumentCaptor<List<Metric>> metricsCaptor;

    @Captor
    private ArgumentCaptor<String> nameCaptor;

    @Mock
    private MetricCacher metricCacher;

    @Mock
    private MetricSearch metricSearch;

    @Mock
    private MetricDescription metricDescription;

    @Before
    public void setup() {
        Mockito.when(metricSearch.add(Mockito.anyString())).thenReturn(metricDescription);
    }

    @Test
    public void savesAccumulatedMetrics() {
        StatisticsCounter counter = new StatisticsCounter("test", 5, metricSearch, metricCacher);
        counter.initialize(null);

        counter.accumulateMetric(AccumulatedMetric.NUMBER_OF_WEB_REQUESTS, 1);
        counter.accumulateMetric(AccumulatedMetric.NUMBER_OF_WEB_REQUESTS, 2);

        counter.flush(new HashMap<>());

        Mockito.verify(metricCacher).submitMetrics(metricsCaptor.capture());
        double valueCounter = metricsCaptor.getValue().stream()
            .map(Metric::getValue)
            .filter(x -> x > 0)
            .findFirst().orElse(0d);

        Assert.assertEquals(3, (int) valueCounter);
    }

    @Test
    public void savesInstantMetrics() {
        StatisticsCounter counter = new StatisticsCounter("test", 5, metricSearch, metricCacher);
        counter.initialize(null);

        Map<InstantMetric, Supplier<Double>> instantMetricsSuppliers = new HashMap<>();
        instantMetricsSuppliers.put(InstantMetric.METRIC_CACHE_QUEUE_SIZE, () -> 2d);
        counter.flush(instantMetricsSuppliers);

        Mockito.verify(metricCacher).submitMetrics(metricsCaptor.capture());
        double valueCounter = metricsCaptor.getValue().stream()
            .map(Metric::getValue)
            .filter(x -> x > 1)
            .findFirst().orElse(0d);

        Assert.assertEquals(2, (int) valueCounter);
    }

    @Test
    public void testInitializeEmptyHostname() {
        StatisticsCounter counter = new StatisticsCounter("test", 5, metricSearch, metricCacher);
        counter.initialize(null);
        Mockito.verify(metricSearch, Mockito.atLeastOnce()).add(nameCaptor.capture());
        nameCaptor.getAllValues().forEach(
            name -> Assert.assertTrue(name.matches("test\\.(accumulated|instant)\\.\\w[\\w_]+\\w"))
        );
    }

    @Test
    public void testInitializeWithHostname() {
        StatisticsCounter counter = new StatisticsCounter("test", 5, metricSearch, metricCacher);
        counter.initialize("test.graphouse-1.com");
        Mockito.verify(metricSearch, Mockito.atLeastOnce()).add(nameCaptor.capture());
        nameCaptor.getAllValues().forEach(
            name -> Assert.assertTrue(name.matches("test\\.hosts\\.test_graphouse-1_com\\.(accumulated|instant)\\.\\w[\\w_]+\\w"))
        );
    }
}
