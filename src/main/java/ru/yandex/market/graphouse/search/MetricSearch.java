package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.market.graphouse.utils.AppendableList;
import ru.yandex.market.graphouse.utils.AppendableResult;
import ru.yandex.market.graphouse.utils.AppendableWrapper;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.monitoring.MonitoringUnit;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    private JdbcTemplate graphouseJdbcTemplate;
    private Monitoring monitoring;
    private MetricValidator metricValidator;

    private MonitoringUnit metricSearchUnit = new MonitoringUnit("MetricSearch");
    private final MetricTree metricTree = new MetricTree();
    private final Queue<MetricDescription> updateQueue = new ConcurrentLinkedQueue<>();

    private int lastUpdatedTimestampSeconds = 0;

    private int saveIntervalSeconds = 180;
    /**
     * Задержка на запись, репликацию, синхронизацию
     */
    private int updateDelaySeconds = 120;

    private volatile boolean metricTreeLoaded = false;

    @Override
    public void afterPropertiesSet() throws Exception {
        initDatabase();
        monitoring.addUnit(metricSearchUnit);
        new Thread(this, "MetricSearch thread").start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Shutting down Metric search");
                saveUpdatedMetrics();
                log.info("Metric search stopped");
            }
        }));
    }

    private void initDatabase() {
        graphouseJdbcTemplate.update(
            "CREATE TABLE IF NOT EXISTS metric (" +
                "  `NAME` VARCHAR(255) NOT NULL, " +
                "  `status` TINYINT NOT NULL DEFAULT 0, " +
                "  `UPDATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                "  PRIMARY KEY (`NAME`), " +
                "  INDEX (`UPDATED`)" +
                ") "
        );
    }

    private void saveMetrics(List<MetricDescription> metrics) {
        if (metrics.isEmpty()) {
            return;
        }

        final String sql = "INSERT INTO metric (name, status) VALUES (?, ?) " +
            "ON DUPLICATE KEY UPDATE status = ?, updated = IF(status != ?, CURRENT_TIMESTAMP, updated)";

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
                    int statusId = metricDescription.getStatus().getId();

                    ps.setString(1, metricDescription.getName());
                    ps.setInt(2, statusId);
                    ps.setInt(3, statusId);
                    ps.setInt(4, statusId);
                }

                @Override
                public int getBatchSize() {
                    return batchList.size();
                }
            };

            graphouseJdbcTemplate.batchUpdate(sql, batchSetter);
        }
    }

    private void loadAllMetrics() {
        log.info("Loading all metric names from db...");
        final AtomicInteger metricCount = new AtomicInteger(0);

        graphouseJdbcTemplate.query(
            "SELECT name, status FROM metric",
            new MetricRowCallbackHandler(metricCount)
        );
        metricTreeLoaded = true;
        log.info("Loaded complete. Total " + metricCount.get() + " metrics");
    }

    private void loadUpdatedMetrics(int startTimestampSeconds) {
        log.info("Loading updated metric names from db...");
        final AtomicInteger metricCount = new AtomicInteger(0);

        graphouseJdbcTemplate.query(
            "SELECT name, status FROM metric WHERE updated >= FROM_UNIXTIME(?)",
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
            MetricStatus status = MetricStatus.forId(rs.getInt("status"));
            if (!metricValidator.validate(metric, true)) {
                log.warn("Invalid metric in db: " + metric);
                return;
            }
            metricTree.modify(metric, status);
            if (metricCount.incrementAndGet() % 100_000 == 0) {
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
            log.error("Failed to save metrics to mysql", e);
            updateQueue.addAll(metrics);
        }
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                log.info(
                    "Actual metrics count = " + metricTree.metricCount() + ", dir count: " + metricTree.dirCount()
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
    }

    public void loadNewMetrics() {
        int timeSeconds = (int) (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())) - updateDelaySeconds;
        if (lastUpdatedTimestampSeconds == 0) {
            loadAllMetrics();
        } else {
            loadUpdatedMetrics(lastUpdatedTimestampSeconds);
        }
        lastUpdatedTimestampSeconds = timeSeconds;
    }

    public MetricDescription add(String metric) {
        long currentTimeMillis = System.currentTimeMillis();
        MetricDescription metricDescription = metricTree.add(metric);
        if (metricDescription != null && metricDescription.getUpdateTimeMillis() >= currentTimeMillis) {
            updateQueue.add(metricDescription);
        }
        return metricDescription;
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
            if (metricDescription != null && metricDescription.getUpdateTimeMillis() >= currentTimeMillis) {
                metricDescriptions.add(metricDescription);
            }
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

    @Required
    public void setGraphouseJdbcTemplate(JdbcTemplate graphouseJdbcTemplate) {
        this.graphouseJdbcTemplate = graphouseJdbcTemplate;
    }

    @Required
    public void setMonitoring(Monitoring monitoring) {
        this.monitoring = monitoring;
    }

    @Required
    public void setMetricValidator(MetricValidator metricValidator) {
        this.metricValidator = metricValidator;
    }

    public void setSaveIntervalSeconds(int saveIntervalSeconds) {
        this.saveIntervalSeconds = saveIntervalSeconds;
    }

    public void setUpdateDelaySeconds(int updateDelaySeconds) {
        this.updateDelaySeconds = updateDelaySeconds;
    }

    public boolean isMetricTreeLoaded() {
        return metricTreeLoaded;
    }
}
