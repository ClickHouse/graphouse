package ru.yandex.market.graphouse.statistics;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.HashMap;
import java.util.List;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 26.12.17
 */
@RunWith(MockitoJUnitRunner.class)
public class StatisticsCounterTest {
    @Captor
    private ArgumentCaptor<List<Metric>> metricsCaptor;

    @Test
    public void savesMetrics() {
        MetricCacher metricCacher = Mockito.mock(MetricCacher.class);
        MetricSearch metricSearch = Mockito.mock(MetricSearch.class);
        MetricDescription metricDescription = Mockito.mock(MetricDescription.class);

        Mockito.when(metricSearch.add(Mockito.anyString())).thenReturn(metricDescription);

        StatisticsCounter counter = new StatisticsCounter("test", 5, metricSearch, metricCacher);
        counter.initialize();

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
}
