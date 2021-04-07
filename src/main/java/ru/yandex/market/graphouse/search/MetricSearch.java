package ru.yandex.market.graphouse.search;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.util.StopWatch;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.data.ClickHouseDirContentLoader;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.monitoring.MonitoringUnit;
import ru.yandex.market.graphouse.retention.RetentionProvider;
import ru.yandex.market.graphouse.save.OnRecordCacheUpdater;
import ru.yandex.market.graphouse.save.UpdateMetricQueueService;
import ru.yandex.market.graphouse.search.tree.DirContent;
import ru.yandex.market.graphouse.search.tree.DirContentBatcher;
import ru.yandex.market.graphouse.search.tree.InMemoryMetricDir;
import ru.yandex.market.graphouse.search.tree.LoadableMetricDir;
import ru.yandex.market.graphouse.search.tree.MetricDescription;
import ru.yandex.market.graphouse.search.tree.MetricDir;
import ru.yandex.market.graphouse.search.tree.MetricDirFactory;
import ru.yandex.market.graphouse.search.tree.MetricName;
import ru.yandex.market.graphouse.search.tree.MetricTree;
import ru.yandex.market.graphouse.server.MetricDescriptionProvider;
import ru.yandex.market.graphouse.statistics.LoadedMetricsCounter;
import ru.yandex.market.graphouse.utils.AppendableList;
import ru.yandex.market.graphouse.utils.AppendableResult;
import ru.yandex.market.graphouse.utils.AppendableWrapper;
import ru.yandex.market.graphouse.utils.TraceAppendableWrapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 07/04/15
 */
public class MetricSearch implements InitializingBean, Runnable, MetricDescriptionProvider {

    private static final Logger log = LogManager.getLogger();

    private final ClickHouseDirContentLoader clickHouseDirContentLoader;
    private final Monitoring monitoring;
    private final Monitoring ping;
    private final MetricValidator metricValidator;
    private final RetentionProvider retentionProvider;
    private final OnRecordCacheUpdater onRecordCacheUpdater;
    private final UpdateMetricQueueService updateMetricQueue;

    private final MonitoringUnit metricSearchUnit = new MonitoringUnit("MetricSearch", 2, TimeUnit.MINUTES);
    private final MonitoringUnit metricTreeInitUnit = new MonitoringUnit("MetricTreeInit");
    private MetricTree metricTree;

    private int lastUpdatedTimestampSeconds = 0;

    @Value("${graphouse.search.refresh-seconds}")
    private int saveIntervalSeconds;

    @Value("${graphouse.search.warn-delay-seconds}")
    private int warnSaveDelaySeconds;

    @Value("${graphouse.search.crit-delay-seconds}")
    private int critSaveDelaySeconds;

    /**
     * Задержка на запись, репликацию, синхронизацию
     */
    private int updateDelaySeconds = 120;

    @Value("${graphouse.tree.in-memory-levels}")
    private int inMemoryLevelsCount;

    @Value("${graphouse.tree.dir-content.cache-time-minutes}")
    private int dirContentCacheTimeMinutes;

    @Value("${graphouse.tree.max-cache-size}")
    private int maxCacheSize;

    @Value("${graphouse.tree.dir-content.batcher.max-parallel-requests}")
    private int dirContentBatcherMaxParallelRequest;

    @Value("${graphouse.tree.dir-content.batcher.max-batch-size}")
    private int dirContentBatcherMaxBatchSize;

    @Value("${graphouse.tree.dir-content.batcher.aggregation-time-millis}")
    private int dirContentBatcherAggregationTimeMillis;

    private String metricsTable;

    @Value("${graphouse.tree.max-subdirs-per-dir}")
    private int maxSubDirsPerDir;

    @Value("${graphouse.tree.max-metrics-per-dir}")
    private int maxMetricsPerDir;

    @Value("${graphouse.search.query-retry-count}")
    private int queryRetryCount;

    @Value("${graphouse.search.query-retry-increment-sec}")
    private int queryRetryIncrementSec;

    @Value("${graphouse.search.max-metrics-per-query}")
    private int maxMetricsPerQuery;

    @Value("${graphouse.on-record-metric-cache.enable}")
    private boolean onRecordCacheEnable;

    private AsyncLoadingCache<MetricDir, DirContent> dirContentProvider;
    private MetricDirFactory metricDirFactory;
    private final Pattern directoriesForSearchCache;

    private DirContentBatcher dirContentBatcher;

    public MetricSearch(
        JdbcTemplate clickHouseJdbcTemplate,
        UpdateMetricQueueService updateMetricQueue,
        Monitoring monitoring,
        Monitoring ping,
        MetricValidator metricValidator,
        RetentionProvider retentionProvider,
        LoadedMetricsCounter loadedMetricsCounter,
        OnRecordCacheUpdater onRecordCacheUpdater,
        String metricsTable,
        String directoriesForSearchCache
    ) {
        this.monitoring = monitoring;
        this.ping = ping;
        this.metricValidator = metricValidator;
        this.retentionProvider = retentionProvider;
        this.onRecordCacheUpdater = onRecordCacheUpdater;
        this.metricsTable = metricsTable;

        this.updateMetricQueue = updateMetricQueue;
        this.clickHouseDirContentLoader = new ClickHouseDirContentLoader(
            clickHouseJdbcTemplate,
            queryRetryCount,
            queryRetryIncrementSec,
            metricsTable,
            loadedMetricsCounter
        );

        monitoring.addUnit(metricSearchUnit);
        ping.addUnit(metricTreeInitUnit);
        metricTreeInitUnit.critical("Initializing");

        if (directoriesForSearchCache == null || directoriesForSearchCache.isEmpty()) {
            this.directoriesForSearchCache = null;
        } else {
            this.directoriesForSearchCache = MetricUtil.createStartWithDirectoryPattern(
                directoriesForSearchCache.split(",")
            );
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        metricDirFactory = (parent, name, status) -> {
            int level = parent.getLevel() + 1;
            if (level < inMemoryLevelsCount) {
                return new InMemoryMetricDir(parent, name, status);
            } else {
                return new LoadableMetricDir(parent, name, status, dirContentProvider);
            }
        };

        dirContentBatcher = new DirContentBatcher(
            this,
            dirContentBatcherMaxParallelRequest,
            dirContentBatcherMaxBatchSize,
            dirContentBatcherAggregationTimeMillis
        );

        Caffeine<Object, Object> dirContentProviderBuilder = Caffeine.newBuilder()
            .recordStats()
            .expireAfterAccess(dirContentCacheTimeMinutes, TimeUnit.MINUTES);

        if (maxCacheSize > 0) {
            dirContentProviderBuilder.maximumSize(maxCacheSize);
        }
        dirContentProvider = dirContentProviderBuilder.buildAsync((dir) -> dirContentBatcher.loadDirContent(dir));

        metricTree = new MetricTree(metricDirFactory, retentionProvider, maxSubDirsPerDir, maxMetricsPerDir);


        if (warnSaveDelaySeconds > 0) {
            metricSearchUnit.setWarningTimeout(warnSaveDelaySeconds, TimeUnit.SECONDS);
        }
        if (critSaveDelaySeconds > 0) {
            metricSearchUnit.setCriticalTimeout(critSaveDelaySeconds, TimeUnit.SECONDS);
        }

        new Thread(this, "MetricSearch thread").start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Metric search");
            updateMetricQueue.saveUpdatedMetrics();
            log.info("Metric search stopped");
        }));
    }

    public Map<MetricDir, DirContent> loadDirsContent(Set<MetricDir> dirs) {
        DirContentRequestRowCallbackHandler metricHandler = new DirContentRequestRowCallbackHandler(dirs);
        clickHouseDirContentLoader.loadDirsContent(dirs, metricHandler);
        return metricHandler.getResult();
    }

    private class DirContentRequestRowCallbackHandler implements ClickHouseDirContentLoader.DirContentRowCallbackHandler {

        private final Map<String, MetricDir> dirNames;
        private final Map<MetricDir, DirContent> result = new HashMap<>();

        private String currentDirName = null;
        private MetricDir currentDir;
        private ConcurrentMap<String, MetricDir> currentDirs;
        private ConcurrentMap<String, MetricName> currentMetrics;
        private int metricCount = 0;
        private int dirCount = 0;


        public DirContentRequestRowCallbackHandler(Set<MetricDir> requestDirs) {
            dirNames = requestDirs.stream().collect(Collectors.toMap(MetricDir::getName, Function.identity()));
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            String dirName = rs.getString("parent");
            checkNewDir(dirName);

            String fullName = rs.getString("name");
            if (!metricValidator.validate(fullName, true)) {
                log.warn("Invalid metric in db: " + fullName);
                return;
            }

            MetricStatus status = MetricStatus.valueOf(rs.getString("last_status"));
            boolean isDir = MetricUtil.isDir(fullName);
            String name = MetricUtil.getLastLevelName(fullName).intern();
            if (isDir) {
                if (maxSubDirsPerDir > 0 && currentDirs.size() >= maxSubDirsPerDir && !status.handmade()) {
                    return;
                }
                currentDirs.put(name, metricDirFactory.createMetricDir(currentDir, name, status));
                dirCount++;
            } else {
                if (maxMetricsPerDir > 0 && currentMetrics.size() >= maxMetricsPerDir && !status.handmade()) {
                    return;
                }
                currentMetrics.put(name, new MetricName(currentDir, name, status, retentionProvider));
                metricCount++;
            }
        }

        private void checkNewDir(String dirName) {
            if (currentDirName == null || !currentDirName.equals(dirName)) {
                flushResult();
                currentDirName = dirName;
                currentDir = dirNames.remove(dirName);
                currentDirs = new ConcurrentHashMap<>();
                currentMetrics = new ConcurrentHashMap<>();
            }
        }

        private void flushResult() {
            if (currentDirName != null && currentDir != null) {
                result.put(currentDir, new DirContent(currentDirs, currentMetrics));
            }
            currentDirName = null;
        }

        public Map<MetricDir, DirContent> getResult() {
            flushResult();
            for (MetricDir metricDir : dirNames.values()) {
                result.put(metricDir, DirContent.createEmpty());
            }
            return result;
        }

        @Override
        public int getMetricCount() {
            return metricCount;
        }

        @Override
        public int getDirCount() {
            return dirCount;
        }
    }

    private void loadAllMetrics() {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        log.info("Loading all metric names from db...");
        int totalMetrics = 0;


        for (int level = 1; ; level++) {
            log.info("Loading metrics for level " + level);
            final AtomicInteger levelCount = new AtomicInteger(0);
            clickHouseDirContentLoader.executeQuery(
                "SELECT name, argMax(status, updated) as last_status " +
                    "FROM " + metricsTable + " PREWHERE level = ? WHERE status != ? GROUP BY name",
                new MetricRowCallbackHandler(levelCount, false),
                level, MetricStatus.AUTO_HIDDEN.name()
            );

            if (levelCount.get() == 0) {
                log.info("No metrics on level " + level + " loading complete");
                break;
            }
            totalMetrics += levelCount.get();
            log.info("Loaded " + levelCount.get() + " metrics for level " + level);

            if (inMemoryLevelsCount > 0 && level == inMemoryLevelsCount) {
                log.info("Loaded first all " + inMemoryLevelsCount + " in memory levels of metric tree");
                break;
            }
        }
        stopWatch.stop();
        log.info(
            "Loaded complete. Total " + totalMetrics + " metrics in " + stopWatch.getTotalTimeSeconds() + " seconds"
        );
        metricTreeInitUnit.ok();
    }

    private void loadUpdatedMetrics(int startTimestampSeconds, int endTimestampSeconds) {
        log.info("Loading updated metric names from db...");

        if (maxMetricsPerQuery > 0) {
            loadUpdatedMetricsWithBatches(startTimestampSeconds, endTimestampSeconds);
        } else {
            loadUpdatedMetricsWithSingleQuery(startTimestampSeconds, endTimestampSeconds);
        }
    }

    private void loadUpdatedMetricsWithSingleQuery(int startTimestampSeconds, int endTimestampSeconds) {
        final AtomicInteger metricCount = new AtomicInteger(0);

        clickHouseDirContentLoader.executeQuery(
            "SELECT name, argMax(status, updated) as last_status FROM " + metricsTable +
                " PREWHERE updated >= toDateTime(?) and updated <= toDateTime(?)" +
                " GROUP BY name",
            new MetricRowCallbackHandler(metricCount, true),
            startTimestampSeconds, endTimestampSeconds
        );
        log.info("Loaded complete. Total " + metricCount.get() + " metrics");
    }

    private void loadUpdatedMetricsWithBatches(int startTimestampSeconds, int endTimestampSeconds) {
        int totalMetricCount = 0;
        AtomicInteger metricCount;
        int offset = 0;

        do {
            metricCount = new AtomicInteger(0);
            clickHouseDirContentLoader.executeQuery(
                "SELECT name, argMax(status, updated) as last_status FROM " + metricsTable +
                    " PREWHERE updated >= toDateTime(?) and updated <= toDateTime(?)" +
                    " GROUP BY name" +
                    " ORDER BY name" +
                    " LIMIT ? OFFSET ?",
                new MetricRowCallbackHandler(metricCount, true),
                startTimestampSeconds, endTimestampSeconds, maxMetricsPerQuery, offset
            );
            offset += maxMetricsPerQuery;
            totalMetricCount += metricCount.get();
            log.info("Loaded " + metricCount.get() + " metrics");
        } while (metricCount.get() > 0);

        log.info("Loaded complete. Total " + totalMetricCount + " metrics");
    }

    private class MetricRowCallbackHandler implements RowCallbackHandler {

        private final boolean updatedMetrics;
        private final AtomicInteger metricCount;

        public MetricRowCallbackHandler(AtomicInteger metricCount, boolean updatedMetrics) {
            this.metricCount = metricCount;
            this.updatedMetrics = updatedMetrics;
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            String metric = rs.getString("name");
            MetricStatus status = MetricStatus.valueOf(rs.getString("last_status"));

            if (onRecordCacheEnable && updatedMetrics) {
                processUpdatedMetric(metric, status);
            } else {
                processMetric(metric, status);
            }
        }

        private void processUpdatedMetric(String metric, MetricStatus status) {
            if (directoriesForSearchCache == null || !directoriesForSearchCache.matcher(metric).find()) {
                updateOnRecordCache(metric, status);
            } else {
                processMetric(metric, status);
            }
        }

        protected void processMetric(String metric, MetricStatus status) {
            if (!metricValidator.validate(metric, true)) {
                log.warn("Invalid metric in db: " + metric);
                return;
            }

            // Only update metrics that are already in memory
            String[] metricLevels = MetricUtil.splitToLevels(metric);
            if (metricLevels.length <= inMemoryLevelsCount || (metricTree.maybeFindParent(metricLevels) != null)) {
                metricTree.modify(metric, status);
            }

            int count = metricCount.incrementAndGet();
            if (count % 500_000 == 0) {
                log.info("Loaded " + metricCount.get() + " metrics...");
            }
        }

        private void updateOnRecordCache(String metric, MetricStatus status) {
            onRecordCacheUpdater.tryToUpdateMetricCache(metric, status);
        }
    }

    @Override
    public void run() {
        log.info("Metric search thread started");
        while (!Thread.interrupted()) {
            try {
                updateMetricsState();
                metricSearchUnit.ok();
            } catch (Exception e) {
                log.error("Failed to update metric search", e);
                metricSearchUnit.critical("Failed to update metric search: " + e.getMessage(), e);
            }
            try {
                TimeUnit.SECONDS.sleep(saveIntervalSeconds);
            } catch (InterruptedException ignored) {
            }
        }
        log.info("Metric search thread finished");
    }

    private void updateMetricsState() {
        CacheStats stats = dirContentProvider.synchronous().stats();
        log.info(
            "Actual metrics count = {} , dir count: {}, hitRate: {}, requestCount: {}, cache stats: {}",
            metricTree.metricCount(), metricTree.dirCount(), stats.hitRate(), stats.requestCount(),
            stats.toString()
        );

        long loadNewMetricsMs = loadNewMetrics();
        long updateMetricsMs = updateMetricQueue.saveUpdatedMetrics();


        long executionSeconds = TimeUnit.MILLISECONDS.toSeconds(loadNewMetricsMs + updateMetricsMs);
        if (executionSeconds > warnSaveDelaySeconds) {
            log.warn(
                "Execution time exceeded: {} s. Load metrics: {} s. Update metrics: {}s.",
                executionSeconds,
                TimeUnit.MILLISECONDS.toSeconds(loadNewMetricsMs),
                TimeUnit.MILLISECONDS.toSeconds(updateMetricsMs)
            );
        }
    }

    public long loadNewMetrics() {
        long startTimestampMs = System.currentTimeMillis();

        int currentTimestampSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        int timeSeconds = currentTimestampSeconds - updateDelaySeconds;
        if (!isMetricTreeLoaded()) {
            loadAllMetrics();
        } else {
            loadUpdatedMetrics(lastUpdatedTimestampSeconds, currentTimestampSeconds);
        }
        lastUpdatedTimestampSeconds = timeSeconds;

        return System.currentTimeMillis() - startTimestampMs;
    }

    public MetricDescription maybeFindMetric(String[] levels) {
        return metricTree.maybeFindMetric(levels);
    }

    @Override
    public MetricDescription getMetricDescription(String name) {
        return add(name);
    }

    public MetricDescription add(String metric) {
        long currentTimeMillis = System.currentTimeMillis();
        MetricDescription metricDescription = metricTree.add(metric);
        updateMetricQueue.addUpdatedMetrics(currentTimeMillis, metricDescription);
        return metricDescription;
    }

    private void addUpdatedMetrics(long startTimeMillis, MetricDescription metricDescription,
                                   Collection<MetricDescription> updatedCollection) {
        if (metricDescription != null && metricDescription.getUpdateTimeMillis() >= startTimeMillis) {
            updatedCollection.add(metricDescription);
            addUpdatedMetrics(startTimeMillis, metricDescription.getParent(), updatedCollection);
        }
    }

    public int multiModify(String query, final MetricStatus status, final Appendable result) throws IOException {
        final AppendableList appendableList = new AppendableList();
        final AtomicInteger count = new AtomicInteger();

        metricTree.search(query, appendableList);

        for (MetricDescription metricDescription : appendableList.getList()) {
            final String metricName = metricDescription.getName();
            modify(metricName, status);
            result.append(metricName);
            count.incrementAndGet();
        }

        return count.get();
    }

    public void modify(String metric, MetricStatus status) {
        modify(Collections.singletonList(metric), status);
    }


    public void modify(List<String> metrics, MetricStatus status) {
        if (metrics == null || metrics.isEmpty()) {
            return;
        }
        if (status == MetricStatus.SIMPLE) {
            throw new IllegalStateException("Cannon modify to SIMPLE status");
        }
        long currentTimeMillis = System.currentTimeMillis();
        List<MetricDescription> metricDescriptions = new ArrayList<>();
        for (String metric : metrics) {
            if (!metricValidator.validate(metric, true)) {
                log.warn("Wrong metric to modify: " + metric);
                continue;
            }
            MetricDescription metricDescription = metricTree.modify(metric, status);

            addUpdatedMetrics(currentTimeMillis, metricDescription, metricDescriptions);

        }
        updateMetricQueue.saveMetrics(metricDescriptions);
        if (metrics.size() == 1) {
            log.info("Updated metric '" + metrics.get(0) + "', status: " + status.name());
        } else {
            log.info("Updated " + metrics.size() + " metrics, status: " + status.name());
        }
    }

    public void search(String query, Appendable result) throws IOException {
        metricTree.search(query, new AppendableWrapper(result));
    }

    public void searchCachedMetrics(String query, Appendable result, boolean writeLoadedInfo) throws IOException {
        metricTree.searchCachedMetrics(query, new TraceAppendableWrapper(result, writeLoadedInfo));
    }

    public void search(String query, AppendableResult result) throws IOException {
        metricTree.search(query, result);
    }

    public boolean isMetricTreeLoaded() {
        return lastUpdatedTimestampSeconds != 0;
    }

    public MonitoringUnit getMetricSearchUnit() {
        return metricSearchUnit;
    }
}
