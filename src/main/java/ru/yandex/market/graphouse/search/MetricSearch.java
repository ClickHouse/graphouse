package ru.yandex.market.graphouse.search;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.util.StopWatch;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.monitoring.MonitoringUnit;
import ru.yandex.market.graphouse.retention.RetentionProvider;
import ru.yandex.market.graphouse.search.tree.DirContent;
import ru.yandex.market.graphouse.search.tree.DirContentBatcher;
import ru.yandex.market.graphouse.search.tree.InMemoryMetricDir;
import ru.yandex.market.graphouse.search.tree.LoadableMetricDir;
import ru.yandex.market.graphouse.search.tree.MetricDescription;
import ru.yandex.market.graphouse.search.tree.MetricDir;
import ru.yandex.market.graphouse.search.tree.MetricDirFactory;
import ru.yandex.market.graphouse.search.tree.MetricName;
import ru.yandex.market.graphouse.search.tree.MetricTree;
import ru.yandex.market.graphouse.utils.AppendableList;
import ru.yandex.market.graphouse.utils.AppendableResult;
import ru.yandex.market.graphouse.utils.AppendableWrapper;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 07/04/15
 */
public class MetricSearch implements InitializingBean, Runnable {

    private static final Logger log = LogManager.getLogger();

    private static final int BATCH_SIZE = 5_000;
    private static final int MAX_METRICS_PER_SAVE = 1_000_000;

    private final JdbcTemplate clickHouseJdbcTemplate;
    private final Monitoring monitoring;
    private final Monitoring ping;
    private final MetricValidator metricValidator;
    private final RetentionProvider retentionProvider;


    private final MonitoringUnit metricSearchUnit = new MonitoringUnit("MetricSearch", 2, TimeUnit.MINUTES);
    private final MonitoringUnit metricTreeInitUnit = new MonitoringUnit("MetricTreeInit");
    private MetricTree metricTree;
    private final Queue<MetricDescription> updateQueue = new ConcurrentLinkedQueue<>();

    private int lastUpdatedTimestampSeconds = 0;

    @Value("${graphouse.search.refresh-seconds}")
    private int saveIntervalSeconds;
    /**
     * Задержка на запись, репликацию, синхронизацию
     */
    private int updateDelaySeconds = 120;

    @Value("${graphouse.tree.in-memory-levels}")
    private int inMemoryLevelsCount;

    @Value("${graphouse.tree.dir-content.cache-time-minutes}")
    private int dirContentCacheTimeMinutes;

    @Value("${graphouse.tree.dir-content.cache-concurrency-level}")
    private int dirContentCacheConcurrencyLevel;

    @Value("${graphouse.tree.dir-content.batcher.max-parallel-requests}")
    private int dirContentBatcherMaxParallelRequest;

    @Value("${graphouse.tree.dir-content.batcher.max-batch-size}")
    private int dirContentBatcherMaxBatchSize;

    @Value("${graphouse.tree.dir-content.batcher.aggregation-time-millis}")
    private int dirContentBatcherAggregationTimeMillis;

    @Value("${graphouse.clickhouse.metric-tree-table}")
    private String metricsTable;

    @Value("${graphouse.tree.max-subdirs-per-dir}")
    private int maxSubDirsPerDir;

    @Value("${graphouse.tree.max-metrics-per-dir}")
    private int maxMetricsPerDir;


    private LoadingCache<MetricDir, DirContent> dirContentProvider;
    private MetricDirFactory metricDirFactory;

    private DirContentBatcher dirContentBatcher;


    public MetricSearch(JdbcTemplate clickHouseJdbcTemplate, Monitoring monitoring, Monitoring ping,
                        MetricValidator metricValidator, RetentionProvider retentionProvider) {
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.monitoring = monitoring;
        this.ping = ping;
        this.metricValidator = metricValidator;
        this.retentionProvider = retentionProvider;
        monitoring.addUnit(metricSearchUnit);
        metricSearchUnit.setWarningTimeout(3 * saveIntervalSeconds, TimeUnit.SECONDS);
        metricSearchUnit.setCriticalTimeout(10 * saveIntervalSeconds, TimeUnit.SECONDS);
        ping.addUnit(metricTreeInitUnit);
        metricTreeInitUnit.critical("Initializing");
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

        dirContentProvider = CacheBuilder.newBuilder()
            .expireAfterAccess(dirContentCacheTimeMinutes, TimeUnit.MINUTES)
            .softValues()
            .recordStats()
            .concurrencyLevel(dirContentCacheConcurrencyLevel)
            .build(new CacheLoader<MetricDir, DirContent>() {
                @Override
                public DirContent load(MetricDir dir) throws Exception {
                    return dirContentBatcher.loadDirContent(dir);
                }
            });

        metricTree = new MetricTree(metricDirFactory, retentionProvider, maxSubDirsPerDir, maxMetricsPerDir);

        new Thread(this, "MetricSearch thread").start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Metric search");
            saveUpdatedMetrics();
            log.info("Metric search stopped");
        }));
    }

    private void saveMetrics(List<MetricDescription> metrics) {
        if (metrics.isEmpty()) {
            return;
        }

        final String sql = "INSERT INTO " + metricsTable
            + " (name, level, parent, status, updated) VALUES (?, ?, ?, ?, ?)";

        final int batchesCount = (metrics.size() - 1) / BATCH_SIZE + 1;

        for (int batchNum = 0; batchNum < batchesCount; batchNum++) {

            int firstIndex = batchNum * BATCH_SIZE;
            int lastIndex = firstIndex + BATCH_SIZE;

            lastIndex = (lastIndex <= metrics.size()) ? lastIndex : metrics.size();
            final List<MetricDescription> batchList = metrics.subList(firstIndex, lastIndex);

            BatchPreparedStatementSetter batchSetter = new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    MetricDescription metricDescription = batchList.get(i);
                    MetricDescription parent = metricDescription.getParent();
                    ps.setString(1, metricDescription.getName());
                    ps.setInt(2, metricDescription.getLevel());
                    ps.setString(3, (parent != null) ? parent.getName() : "");
                    ps.setString(4, metricDescription.getStatus().name());
                    ps.setTimestamp(5, new Timestamp(metricDescription.getUpdateTimeMillis()));
                }

                @Override
                public int getBatchSize() {
                    return batchList.size();
                }
            };

            clickHouseJdbcTemplate.batchUpdate(sql, batchSetter);
        }
    }

    public Map<MetricDir, DirContent> loadDirsContent(Set<MetricDir> dirs) {

        Stopwatch stopwatch = Stopwatch.createStarted();

        String dirFilter = dirs.stream().map(MetricDir::getName).collect(Collectors.joining("','", "'", "'"));

        DirContentRequestRowCallbackHandler metricHandler = new DirContentRequestRowCallbackHandler(dirs);
        clickHouseJdbcTemplate.query(
            "SELECT parent, name, argMax(status, updated) AS last_status FROM " + metricsTable +
                " PREWHERE parent IN (" + dirFilter + ") WHERE status != ? GROUP BY parent, name ORDER BY parent",
            metricHandler,
            MetricStatus.AUTO_HIDDEN.name()
        );


        stopwatch.stop();
        log.info(
            "Loaded metrics for " + dirs.size() + " dirs: " + dirs
                + " (" + metricHandler.getDirCount() + " dirs, "
                + metricHandler.getMetricCount() + " metrics) in " + stopwatch.toString()
        );
        return metricHandler.getResult();

    }

    private class DirContentRequestRowCallbackHandler implements RowCallbackHandler {

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
            if (currentDirName != null) {
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

        public int getMetricCount() {
            return metricCount;
        }

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
            clickHouseJdbcTemplate.query(
                "SELECT name, argMax(status, updated) as last_status " +
                    "FROM " + metricsTable + " PREWHERE level = ? WHERE status != ? GROUP BY name",
                new MetricRowCallbackHandler(levelCount),
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

    private void loadUpdatedMetrics(int startTimestampSeconds) {
        log.info("Loading updated metric names from db...");
        final AtomicInteger metricCount = new AtomicInteger(0);

        clickHouseJdbcTemplate.query(
            "SELECT name, argMax(status, updated) as last_status FROM " + metricsTable +
                " PREWHERE updated >= toDateTime(?) GROUP BY name",
            new MetricRowCallbackHandler(metricCount),
            startTimestampSeconds
        );
        log.info("Loaded complete. Total " + metricCount.get() + " metrics");
    }

    private class MetricRowCallbackHandler implements RowCallbackHandler {

        private final AtomicInteger metricCount;

        public MetricRowCallbackHandler(AtomicInteger metricCount) {
            this.metricCount = metricCount;
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            String metric = rs.getString("name");
            MetricStatus status = MetricStatus.valueOf(rs.getString("last_status"));
            processMetric(metric, status);

        }

        protected void processMetric(String metric, MetricStatus status) {
            if (!metricValidator.validate(metric, true)) {
                log.warn("Invalid metric in db: " + metric);
                return;
            }
            metricTree.modify(metric, status);
            int count = metricCount.incrementAndGet();
            if (count % 500_000 == 0) {
                log.info("Loaded " + metricCount.get() + " metrics...");
            }
        }
    }

    private void saveUpdatedMetrics() {
        if (updateQueue.isEmpty()) {
            log.info("No new metric names to save");
            return;
        }
        log.info("Saving new metric names to db. Current count: " + updateQueue.size());
        List<MetricDescription> metrics = new ArrayList<>();
        MetricDescription metric;
        while (metrics.size() <= MAX_METRICS_PER_SAVE && (metric = updateQueue.poll()) != null) {
            metrics.add(metric);
        }
        try {
            saveMetrics(metrics);
            log.info("Saved " + metrics.size() + " metric names");
        } catch (Exception e) {
            log.error("Failed to save metrics to database", e);
            updateQueue.addAll(metrics);
        }
    }

    @Override
    public void run() {
        log.info("Metric search thread started");
        while (!Thread.interrupted()) {
            try {
                log.info(
                    "Actual metrics count = " + metricTree.metricCount() + ", dir count: " + metricTree.dirCount()
                        + ", cache stats: " + dirContentProvider.stats().toString()
                );
                loadNewMetrics();
                saveUpdatedMetrics();
                metricSearchUnit.ok();
            } catch (Exception e) {
                log.error("Failed to update metric search", e);
                metricSearchUnit.critical("Failed to update metric search: " + e.getMessage(), e);
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(saveIntervalSeconds));
            } catch (InterruptedException ignored) {
            }
        }
        log.info("Metric search thread finished");

    }

    public void loadNewMetrics() {
        int timeSeconds = (int) (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())) - updateDelaySeconds;
        if (isMetricTreeLoaded()) {
            loadUpdatedMetrics(lastUpdatedTimestampSeconds);
        } else {
            loadAllMetrics();
        }
        lastUpdatedTimestampSeconds = timeSeconds;
    }

    public MetricDescription maybeFindMetric(String[] levels) {
        return metricTree.maybeFindMetric(levels);
    }

    public MetricDescription add(String metric) {
        long currentTimeMillis = System.currentTimeMillis();
        MetricDescription metricDescription = metricTree.add(metric);
        addUpdatedMetrics(currentTimeMillis, metricDescription, updateQueue);
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
        saveMetrics(metricDescriptions);
        if (metrics.size() == 1) {
            log.info("Updated metric '" + metrics.get(0) + "', status: " + status.name());
        } else {
            log.info("Updated " + metrics.size() + " metrics, status: " + status.name());
        }
    }

    public void search(String query, Appendable result) throws IOException {
        metricTree.search(query, new AppendableWrapper(result));
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
