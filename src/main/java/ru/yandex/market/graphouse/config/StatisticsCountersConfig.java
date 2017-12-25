package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.statistics.StatisticsCounter;
import ru.yandex.market.graphouse.statistics.StatisticsFlushFrequencyConfig;
import ru.yandex.market.graphouse.statistics.StatisticsService;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 22.12.17
 */
@Configuration
@Import({StatisticsConfig.class})
public class StatisticsCountersConfig {
    @Autowired
    StatisticsService statisticsService;

    @Autowired
    StatisticsFlushFrequencyConfig config;

    @Autowired
    MetricSearch metricSearch;

    @Autowired
    MetricCacher metricCacher;

    @Value("${graphouse.statistics.metric_prefix}")
    private String metricsPrefix;

    @PostConstruct
    public void initialize() {
        List<StatisticsCounter> counters = config.getMetricNameToFlushPeriodInSeconds().entrySet()
            .stream()
            .map(this::createCounter)
            .collect(Collectors.toList());

        statisticsService.initialize(counters);
    }

    private StatisticsCounter createCounter(Map.Entry<String, Integer> metricToFlushPeriod) {
        return new StatisticsCounter(
            String.format("%s.%s", metricsPrefix, metricToFlushPeriod.getKey()), metricToFlushPeriod.getValue(),
            metricSearch, metricCacher);
    }
}
