package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.data.MetricDataService;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.retention.ClickHouseRetentionProvider;
import ru.yandex.market.graphouse.retention.DefaultRetentionProvider;
import ru.yandex.market.graphouse.retention.RetentionProvider;
import ru.yandex.market.graphouse.save.OnRecordCacheUpdater;
import ru.yandex.market.graphouse.save.OnRecordMetricProvider;
import ru.yandex.market.graphouse.save.UpdateMetricQueueService;
import ru.yandex.market.graphouse.save.banned.BannedMetricCache;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.server.OnRecordCacheBasedMetricFactory;
import ru.yandex.market.graphouse.server.SearchCacheBasedMetricFactory;
import ru.yandex.market.graphouse.statistics.LoadedMetricsCounter;
import ru.yandex.market.graphouse.statistics.StatisticsService;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
@Configuration
@Import({StatisticsConfig.class})
public class MetricsConfig {

    @Autowired
    private Monitoring monitoring;

    @Autowired
    @Qualifier("ping")
    private Monitoring ping;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplateOnRecord;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplateSearch;

    @Autowired
    private StatisticsService statisticsService;

    @Autowired
    private LoadedMetricsCounter loadedMetricsCounter;

    @Value("${graphouse.clickhouse.data-table}")
    private String graphiteDataTable;

    @Value("${graphouse.clickhouse.retention-config}")
    private String retentionConfig;

    @Value("${graphouse.metric-validation.min-length}")
    private int minMetricLength;

    @Value("${graphouse.metric-validation.max-length}")
    private int maxMetricLength;

    @Value("${graphouse.metric-validation.min-levels}")
    private int minDots;

    @Value("${graphouse.metric-validation.max-levels}")
    private int maxDots;

    @Value("${graphouse.metric-validation.regexp}")
    private String metricRegexp;

    @Value("${graphouse.clickhouse.metric-tree-table}")
    private String metricsTable;

    @Value("${graphouse.on-record-metric-cache.enable}")
    private boolean onRecordCacheEnable;

    @Value("${graphouse.update-metric.apply-current-time.enable}")
    private boolean applyCurrentTimeWhenUpdatingMetrics;

    @Bean
    public UpdateMetricQueueService updateMetricQueueService() {
        return new UpdateMetricQueueService(
            statisticsService,
            metricsTable,
            applyCurrentTimeWhenUpdatingMetrics,
            clickHouseJdbcTemplateSearch
        );
    }

    @Bean
    public MetricSearch metricSearch(
        OnRecordCacheUpdater onRecordCacheUpdater,
        UpdateMetricQueueService updateMetricQueueService,
        @Value("${graphouse.search.directories-for-cache:}") String directoriesForSearchCache,
        @Value("${graphouse.search.query-retry-count}") int queryRetryCount,
        @Value("${graphouse.search.query-retry-increment-sec}") int queryRetryIncrementSec
    ) {
        return new MetricSearch(
            clickHouseJdbcTemplateSearch, queryRetryCount, queryRetryIncrementSec,
            updateMetricQueueService, monitoring, ping,
            metricValidator(), retentionProvider(), loadedMetricsCounter,
            onRecordCacheUpdater, metricsTable, directoriesForSearchCache
        );
    }

    @Bean
    public RetentionProvider retentionProvider() {
        if (retentionConfig.isEmpty()) {
            return new DefaultRetentionProvider();
        } else {
            return new ClickHouseRetentionProvider(clickHouseJdbcTemplate, retentionConfig);
        }
    }

    @Bean
    public MetricDataService dataService(
        @Value("${graphouse.clickhouse.data-read-table}") String graphiteDataReadTable,
        @Value("${graphouse.metric-data.max-points-per-metric}") int maxPointsPerMetric,
        MetricSearch metricSearch
    ) {
        return new MetricDataService(
            metricSearch, clickHouseJdbcTemplate, graphiteDataReadTable, maxPointsPerMetric
        );
    }

    @Bean
    public MetricValidator metricValidator() {
        return new MetricValidator(metricRegexp, minMetricLength, maxMetricLength, minDots, maxDots);
    }

    @Bean
    public MetricCacher metricCacher() {
        return new MetricCacher(clickHouseJdbcTemplate, monitoring, statisticsService);
    }

    @Bean
    public SearchCacheBasedMetricFactory metricFactory(
        @Value("${graphouse.host-metric-redirect.enabled}") boolean redirectHostMetrics,
        @Value("${graphouse.host-metric-redirect.dir}") String hostMetricDir,
        @Value("${graphouse.host-metric-redirect.postfixes}") String hostPostfixes,
        MetricSearch metricSearch
    ) {
        return new SearchCacheBasedMetricFactory(
            metricSearch,
            metricValidator(),
            redirectHostMetrics,
            hostMetricDir,
            hostPostfixes
        );
    }

    @Bean
    public OnRecordCacheBasedMetricFactory onRecordCacheBasedMetricFactory(
        @Value("${graphouse.host-metric-redirect.enabled}") boolean redirectHostMetrics,
        @Value("${graphouse.host-metric-redirect.dir}") String hostMetricDir,
        @Value("${graphouse.host-metric-redirect.postfixes}") String hostPostfixes,
        SearchCacheBasedMetricFactory searchCacheBasedMetricFactory,
        OnRecordMetricProvider onRecordMetricProvider,
        @Value("${graphouse.search.directories-for-cache:}") String directoriesForSearchCache
    ) {
        return new OnRecordCacheBasedMetricFactory(
            metricValidator(),
            redirectHostMetrics,
            hostMetricDir,
            hostPostfixes,
            searchCacheBasedMetricFactory,
            onRecordMetricProvider,
            onRecordCacheEnable,
            directoriesForSearchCache
        );
    }

    @Bean
    public BannedMetricCache bannedMetricCache() {
        return new BannedMetricCache();
    }

    @Bean
    public OnRecordMetricProvider onRecordMetricProvider(
        BannedMetricCache bannedMetricCache,
        UpdateMetricQueueService updateMetricQueueService,
        @Value("${graphouse.on-record-metric-provider.cache-expire-time-minutes}") int cacheExpireTimeMinutes,
        @Value("${graphouse.on-record-metric-provider.max-cache-size}") int maxCacheSize,
        @Value("${graphouse.tree.max-subdirs-per-dir}") int maxSubDirsPerDir,
        @Value("${graphouse.tree.max-metrics-per-dir}") int maxMetricsPerDir,
        @Value("${graphouse.on-record-metric-provider.batcher.query-retry-count}") int queryRetryCount,
        @Value("${graphouse.on-record-metric-provider.batcher.query-retry-increment-sec}") int queryRetryIncrementSec,
        @Value("${graphouse.on-record-metric-provider.batcher.max-parallel-requests}") int maxParallelRequests,
        @Value("${graphouse.on-record-metric-provider.batcher.max-batches-count}") int maxMaxBatchesCount,
        @Value("${graphouse.on-record-metric-provider.batcher.max-batch-size}") int maxBatchSize,
        @Value("${graphouse.on-record-metric-provider.batcher.aggregation-time-millis}") int batchAggregationTimeMillis
    ) {
        return new OnRecordMetricProvider(
            onRecordCacheEnable,
            bannedMetricCache,
            loadedMetricsCounter,
            updateMetricQueueService,
            clickHouseJdbcTemplateOnRecord,
            metricValidator(),
            metricsTable,
            cacheExpireTimeMinutes,
            maxCacheSize,
            maxSubDirsPerDir,
            maxMetricsPerDir,
            queryRetryCount,
            queryRetryIncrementSec,
            maxParallelRequests,
            maxMaxBatchesCount,
            maxBatchSize,
            batchAggregationTimeMillis
        );
    }

    @Bean
    public OnRecordCacheUpdater onRecordCacheUpdater(
        OnRecordMetricProvider metricProvider,
        BannedMetricCache bannedMetricCache,
        @Value("${graphouse.on-record-metric-cache.refresh-seconds}") int refreshSeconds,
        @Value("${graphouse.on-record-metric-cache.query-retry-count}") int queryRetryCount,
        @Value("${graphouse.on-record-metric-cache.max-batch-size}") long maxBatchSize
    ) {
        return new OnRecordCacheUpdater(
            metricProvider,
            bannedMetricCache,
            onRecordCacheEnable,
            ping,
            monitoring,
            maxDots + 1,
            clickHouseJdbcTemplate,
            metricsTable,
            refreshSeconds,
            queryRetryCount,
            maxBatchSize
        );
    }
}
