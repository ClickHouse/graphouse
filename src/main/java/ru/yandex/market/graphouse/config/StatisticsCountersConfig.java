package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.statistics.StatisticsCounter;
import ru.yandex.market.graphouse.statistics.StatisticsFlushFrequencyConfig;
import ru.yandex.market.graphouse.statistics.StatisticsServiceImpl;

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
    StatisticsServiceImpl statisticsService;

    @Autowired
    StatisticsFlushFrequencyConfig config;

    @Autowired
    MetricSearch metricSearch;

    @Autowired
    MetricCacher metricCacher;

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
            metricToFlushPeriod.getKey(), metricToFlushPeriod.getValue(), metricSearch, metricCacher
        );
    }
}
