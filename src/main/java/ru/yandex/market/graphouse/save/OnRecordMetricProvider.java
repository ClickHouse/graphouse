package ru.yandex.market.graphouse.save;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.save.banned.BannedMetricCache;
import ru.yandex.market.graphouse.save.tree.OnReadDirContent;
import ru.yandex.market.graphouse.save.tree.OnReadDirContentBatcher;
import ru.yandex.market.graphouse.save.tree.OnRecordMetricDescription;
import ru.yandex.market.graphouse.save.tree.OnRecordMetricTree;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.MetricDescription;
import ru.yandex.market.graphouse.server.MetricDescriptionProvider;
import ru.yandex.market.graphouse.statistics.LoadedMetricsCounter;
import ru.yandex.market.graphouse.utils.OnReadAppendableResult;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class OnRecordMetricProvider implements InitializingBean, Runnable, MetricDescriptionProvider {
    private static final Logger log = LogManager.getLogger();

    private final boolean cacheEnable;
    private final BannedMetricCache metricCache;

    private final OnRecordMetricTree metricTree;
    private final OnReadDirContentBatcher dirContentBatcher;
    private final AsyncLoadingCache<OnRecordMetricDescription, OnReadDirContent> dirContentProvider;

    public OnRecordMetricProvider(
        boolean cacheEnable,
        BannedMetricCache metricCache,
        LoadedMetricsCounter loadedMetricsCounter,
        UpdateMetricQueueService updateMetricQueueService,
        JdbcTemplate clickHouseJdbcTemplate,
        MetricValidator metricValidator,
        String metricsTable,
        int cacheExpireTimeMinutes,
        int maxCacheSize,
        int maxSubDirsPerDir,
        int maxMetricsPerDir,
        int queryRetryCount,
        int queryRetryIncrementSec,
        int maxParallelRequests,
        int maxMaxBatchesCount,
        int maxBatchSize,
        int batchAggregationTimeMillis
    ) {
        this.cacheEnable = cacheEnable;
        this.metricCache = metricCache;

        Caffeine<Object, Object> savedMetricsCacheBuilder = Caffeine.newBuilder()
            .recordStats()
            .expireAfterAccess(cacheExpireTimeMinutes, TimeUnit.MINUTES);

        if (maxCacheSize > 0) {
            savedMetricsCacheBuilder.maximumSize(maxCacheSize);
        }
        dirContentProvider = savedMetricsCacheBuilder.buildAsync(this::createDirContent);

        dirContentBatcher =
            new OnReadDirContentBatcher(
                metricValidator, metricCache, loadedMetricsCounter,
                clickHouseJdbcTemplate, queryRetryCount, queryRetryIncrementSec,
                maxParallelRequests, maxMaxBatchesCount, maxBatchSize,
                metricsTable, maxSubDirsPerDir, maxMetricsPerDir,
                dirContentProvider, batchAggregationTimeMillis
            );

        this.metricTree = new OnRecordMetricTree(
            updateMetricQueueService, metricCache, dirContentProvider, maxSubDirsPerDir, maxMetricsPerDir
        );
    }

    private OnReadDirContent createDirContent(OnRecordMetricDescription metricDescription) {
        return OnReadDirContent.createEmpty(dirContentBatcher);
    }

    @Override
    public void afterPropertiesSet() {
        if (cacheEnable) {
            new Thread(this, "OnRecordMetricProvider thread").start();
        } else {
            log.info("OnRecordMetricProvider disabled");
        }
    }

    @Override
    public void run() {
        log.info("OnRecordMetricProvider thread started");
        while (!Thread.interrupted()) {
            CacheStats stats = dirContentProvider.synchronous().stats();
            log.info(
                "Saved metrics cache hitRate: {}, requestCount: {}, cache stats: {}",
                stats.hitRate(), stats.requestCount(), stats.toString()
            );
            try {
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException ignored) {
            }
        }
        log.info("OnRecordMetricProvider thread finished");
    }

    public void traceMetricStateInCache(String metricName, Appendable result) throws IOException {
        OnReadAppendableResult onReadAppendableResult = new OnReadAppendableResult(result);
        result.append("Banned cache:\n");
        metricCache.traceMetricStateInCache(metricName, onReadAppendableResult);
        result.append("Tree cache:\n");
        metricTree.traceMetricStateInCache(metricName, onReadAppendableResult);
    }

    public String printBannedCacheState() {
        return metricCache.printCacheState();
    }

    @Override
    public MetricDescription getMetricDescription(String name) {
        if (metricCache.isBanned(name)) {
            return null;
        }

        return metricTree.getOrCreateMetric(name);
    }

    public void updateMetricIfLoaded(String name, MetricStatus status) {
        metricTree.updateMetricIfLoaded(name, status);
    }

    public void removeMetricFromTree(String name) {
        metricTree.removeMetricFromTree(name);
    }
}
