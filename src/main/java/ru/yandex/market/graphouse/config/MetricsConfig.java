package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.data.MetricDataService;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.server.MetricFactory;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
@Configuration
public class MetricsConfig {

    @Value("${graphouse.search.refresh-seconds}")
    private int searchRefreshSeconds;

    @Value("${graphouse.server.metric_table}")
    private String metricsTable;

    @Value("${graphite.metric.data.table}")
    private String metricDataTable;

    @Value("${graphouse.metric-validation.min-length}")
    private int metricValidationMinLen;

    @Value("${graphouse.metric-validation.max-length}")
    private int metricValidationMaxLen;

    @Value("${graphouse.metric-validation.min-dots}")
    private int metricValidationMinDots;

    @Value("${graphouse.metric-validation.max-dots}")
    private int metricValidationMaxDots;

    @Value("${graphouse.metric-validation.regexp}")
    private String metricValidationRegexp;

    @Value("${graphouse.cacher.cache-size}")
    private int cacheSize;

    @Value("${graphouse.cacher.batch-size}")
    private int cacheBatchSize;

    @Value("${graphouse.cacher.writers-count}")
    private int cacheWritersCount;

    @Value("${graphouse.cacher.flush-interval-seconds}")
    private int cacheFlushIntervalSeconds;

    @Value("${graphouse.host-metric-redirect.enabled}")
    private boolean hostMetricRedirectEnabled;

    @Value("${graphouse.host-metric-redirect.dir}")
    private String hostMetricRedirectDir;

    @Value("${graphouse.host-metric-redirect.postfixes}")
    private String hostMetricRedirectPostfixes;

    @Autowired
    private Monitoring monitoring;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate;

    @Bean
    public MetricSearch metricSearch(MetricValidator metricValidator) {
        final MetricSearch metricSearch = new MetricSearch(clickHouseJdbcTemplate, monitoring, metricValidator);
        metricSearch.setSaveIntervalSeconds(searchRefreshSeconds);
        metricSearch.setMetricsTable(metricsTable);

        return metricSearch;
    }

    @Bean
    public MetricDataService metricDataService() {
        return new MetricDataService(clickHouseNamedJdbcTemplate, metricDataTable);
    }

    @Bean
    public MetricValidator metricValidator() {
        final MetricValidator metricValidator = new MetricValidator();
        metricValidator.setMinMetricLength(metricValidationMinLen);
        metricValidator.setMaxMetricLength(metricValidationMaxLen);
        metricValidator.setMinDots(metricValidationMinDots);
        metricValidator.setMaxDots(metricValidationMaxDots);
        metricValidator.setMetricRegexp(metricValidationRegexp);

        return metricValidator;
    }

    @Bean
    public MetricCacher metricCacher() {
        final MetricCacher metricCacher = new MetricCacher(clickHouseJdbcTemplate, monitoring, metricDataTable);
        metricCacher.setCacheSize(cacheSize);
        metricCacher.setBatchSize(cacheBatchSize);
        metricCacher.setWritersCount(cacheWritersCount);
        metricCacher.setFlushIntervalSeconds(cacheFlushIntervalSeconds);

        return metricCacher;
    }

    @Bean
    public MetricFactory metricFactory(MetricValidator metricValidator) {
        final MetricFactory metricFactory = new MetricFactory(metricSearch(metricValidator), metricValidator);
        metricFactory.setRedirectHostMetrics(hostMetricRedirectEnabled);
        metricFactory.setHostMetricDir(hostMetricRedirectDir);
        metricFactory.setHostPostfixes(hostMetricRedirectPostfixes);

        return metricFactory;
    }
}
