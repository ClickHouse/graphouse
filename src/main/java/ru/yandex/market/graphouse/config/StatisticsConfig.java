package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.statistics.IStatisticsService;
import ru.yandex.market.graphouse.statistics.StatisticsCounter;
import ru.yandex.market.graphouse.statistics.StatisticsFlushFrequencyConfig;
import ru.yandex.market.graphouse.statistics.StatisticsService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 22.12.17
 */
@Configuration
@Import({MetricsConfig.class})
public class StatisticsConfig {
    @Value("${graphouse.statistics.flush.threads}")
    private int numberOfFlushThreads;

    @Value("${graphouse.statistics.metrics_to_flush_frequency}")
    private String metricsFlushFrequency;


    @Value("${graphouse.statistics.metric_prefix}")
    private String metricsPrefix;

    @Bean
    public StatisticsFlushFrequencyConfig statisticsFlushFrequencyConfig() {
        return new StatisticsFlushFrequencyConfig(metricsFlushFrequency);
    }

    @Bean
    public IStatisticsService statisticsService(StatisticsFlushFrequencyConfig config, MetricSearch metricSearch,
                                                MetricCacher metricCacher) {
        List<StatisticsCounter> counters = config.getMetricNameToFlushPeriodInSeconds().entrySet()
            .stream()
            .map(e -> new StatisticsCounter(
                String.format("%s.%s", metricsPrefix, e.getKey()), e.getValue(), metricSearch, metricCacher)
            )
            .collect(Collectors.toList());

        return new StatisticsService(counters, numberOfFlushThreads);
    }
}
