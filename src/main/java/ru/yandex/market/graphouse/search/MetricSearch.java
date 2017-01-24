package ru.yandex.market.graphouse.search;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.util.StopWatch;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.monitoring.MonitoringUnit;
import ru.yandex.market.graphouse.search.tree.DirContent;
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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final MetricValidator metricValidator;

    private MonitoringUnit metricSearchUnit = new MonitoringUnit("MetricSearch");
    private MetricTree metricTree;
    private final Queue<MetricDescription> updateQueue = new ConcurrentLinkedQueue<>();


    private int lastUpdatedTimestampSeconds = 0;

    private int saveIntervalSeconds = 180;
    /**
     * Задержка на запись, репликацию, синхронизацию
     */
    private int updateDelaySeconds = 120;

    private int startupThreadsCount = 12;

    private int inMemoryLevelsCount = 3;
    private int dirContentCacheTimeMinutes = 60;
    private int dirContentCacheConcurrencyLevel = 6;

    private LoadingCache<MetricDir, DirContent> dirContentProvider;

    private String metricsTable;
    private MetricDirFactory metricDirFactory;

    public MetricSearch(JdbcTemplate clickHouseJdbcTemplate, Monitoring monitoring, MetricValidator metricValidator) {
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.monitoring = monitoring;
        this.metricValidator = metricValidator;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        monitoring.addUnit(metricSearchUnit);

        metricDirFactory = (parent, name, status) -> {
            int level = parent.getLevel() + 1;
            if (level < inMemoryLevelsCount) {
                return new InMemoryMetricDir(parent, name, status);
            } else {
                return new LoadableMetricDir(parent, name, status, dirContentProvider);
            }
        };

        dirContentProvider = CacheBuilder.newBuilder()
            .expireAfterAccess(dirContentCacheTimeMinutes, TimeUnit.MINUTES)
            .softValues()
            .recordStats()
            .concurrencyLevel(dirContentCacheConcurrencyLevel)
            .build(new CacheLoader<MetricDir, DirContent>() {
                @Override
                public DirContent load(MetricDir key) throws Exception {
                    return loadDirContent(key);
                }
            });

        metricTree = new MetricTree(metricDirFactory);

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

        final String sql = "INSERT INTO " + metricsTable + " (name, level, parent, status, updated) VALUES (?, ?, ?)";

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


    private DirContent loadDirContent(MetricDir dir) {
        try {
            Map<String, MetricDir> dirs = new ConcurrentHashMap<>();
            Map<String, MetricName> metrics = new ConcurrentHashMap<>();

            Stopwatch stopwatch = Stopwatch.createStarted();
            String dirName = dir.getName();
            clickHouseJdbcTemplate.query(
                "SELECT name, argMax(status, updated) AS last_status, level FROM " + metricsTable +
                    " PREWHERE level = ? AND parent = ? WHERE status != ? GROUP BY level, name ORDER BY level",
                rs -> {
                    String fullName = rs.getString("name");
                    if (!metricValidator.validate(fullName, true)) {
                        log.warn("Invalid metric in db: " + fullName);
                        return;
                    }
                    MetricStatus status = MetricStatus.valueOf(rs.getString("last_status"));
                    boolean isDir = MetricUtil.isDir(fullName);
                    String splits[] = MetricUtil.splitToLevels(fullName);
                    String name = splits[splits.length - 1].intern();
                    if (isDir) {
                        dirs.put(name, metricDirFactory.createMetricDir(dir, name, status));
                    } else {
                        metrics.put(name, new MetricName(dir, name, status));
                    }
                },
                dir.getLevel() + 1, dirName, MetricStatus.AUTO_HIDDEN.name()
            );
            stopwatch.stop();
            log.info(
                "Loaded metrics for dir " + dirName
                    + " (" + dirs.size() + " dirs, " + metrics.size() + " metrics) in " + stopwatch.toString()
            );
            return new DirContent(dirs, metrics);
        } catch (Exception e) {
            log.error("Failed to load content for dir: " + dir.getName(), e);
            throw e;
        }
    }

    private void loadAllMetrics() {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        log.info("Loading all metric names from db...");
        final AtomicInteger metricCount = new AtomicInteger(0);

        String prewere = inMemoryLevelsCount >= 0 ? ("PREWHERE level <= " + inMemoryLevelsCount) : "";

        try (
            StartupRowCallbackHandler handler = new StartupRowCallbackHandler(metricCount, startupThreadsCount, BATCH_SIZE)
        ) {
            clickHouseJdbcTemplate.query(
                "SELECT name, argMax(status, updated) as last_status " +
                    "FROM " + metricsTable + " " + prewere + " WHERE status != ? GROUP BY name",
                handler,
                MetricStatus.AUTO_HIDDEN.name()
            );
        } catch (Exception e) {
            log.error("Error while loading metric tree", e);
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        log.info("Loaded complete. Total " + metricCount.get() + " metrics in " + stopWatch.getTotalTimeSeconds() + " seconds");
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

        final AtomicInteger metricCount;

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
            doProcessMetric(metric, status);
        }

        protected void doProcessMetric(String metric, MetricStatus status) {
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

    private class StartupRowCallbackHandler extends MetricRowCallbackHandler implements AutoCloseable {

        private final int batchSize;
        private final ExecutorService startupExecutorService;

        private List<String> metrics;
        private List<MetricStatus> statuses;

        public StartupRowCallbackHandler(AtomicInteger metricCount, int threadCount, int batchSize) {
            super(metricCount);
            this.batchSize = batchSize;
            startupExecutorService = Executors.newFixedThreadPool(threadCount);
            createLists();
        }

        private void createLists() {
            metrics = new ArrayList<>(BATCH_SIZE);
            statuses = new ArrayList<>(BATCH_SIZE);
        }

        @Override
        protected void processMetric(String metric, MetricStatus status) {
            metrics.add(metric);
            statuses.add(status);
            if (metrics.size() >= batchSize) {
                startupExecutorService.submit(new Worker(metrics, statuses));
                createLists();
            }
        }

        @Override
        public void close() throws Exception {
            startupExecutorService.submit(new Worker(metrics, statuses));
            startupExecutorService.shutdown();
            while (!startupExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
                log.info("Metrics read finished, waiting workers...");
            }
        }

        private class Worker implements Runnable {
            private final List<String> metrics;
            private final List<MetricStatus> statuses;

            public Worker(List<String> metrics, List<MetricStatus> statuses) {
                this.metrics = metrics;
                this.statuses = statuses;
            }

            @Override
            public void run() {
                try {
                    for (int i = 0; i < metrics.size(); i++) {
                        doProcessMetric(metrics.get(i), statuses.get(i));
                    }
                } catch (Exception e) {
                    log.error("Exception in metric worker.", e);
                }
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

    public void setSaveIntervalSeconds(int saveIntervalSeconds) {
        this.saveIntervalSeconds = saveIntervalSeconds;
    }

    public void setUpdateDelaySeconds(int updateDelaySeconds) {
        this.updateDelaySeconds = updateDelaySeconds;
    }

    public void setMetricsTable(String metricsTable) {
        this.metricsTable = metricsTable;
    }

    public void setInMemoryLevelsCount(int inMemoryLevelsCount) {
        this.inMemoryLevelsCount = inMemoryLevelsCount;
    }

    public boolean isMetricTreeLoaded() {
        return lastUpdatedTimestampSeconds != 0;
    }
}
