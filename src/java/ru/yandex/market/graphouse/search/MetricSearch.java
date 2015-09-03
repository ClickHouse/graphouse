package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.common.util.db.BulkUpdater;
import ru.yandex.market.graphite.MetricValidator;
import ru.yandex.market.monitoring.ComplicatedMonitoring;
import ru.yandex.market.monitoring.MonitoringUnit;

import java.io.IOException;
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
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 07/04/15
 */
public class MetricSearch implements InitializingBean, Runnable {

    private static final Logger log = LogManager.getLogger();

    private static final int BATCH_SIZE = 5_000;
    private static final int MAX_METRICS_PER_SAVE = 1_000_000;

    private JdbcTemplate graphouseJdbcTemplate;
    private ComplicatedMonitoring monitoring;
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
                "  `NAME` VARCHAR(200) NOT NULL, " +
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
        BulkUpdater bulkUpdater = new BulkUpdater(
            graphouseJdbcTemplate,
            "INSERT INTO metric (name, status) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE status = ?, updated = IF(status != ?, CURRENT_TIMESTAMP, updated)",
            BATCH_SIZE
        );
        for (MetricDescription metricDescription : metrics) {
            int statusId = metricDescription.getStatus().getId();
            bulkUpdater.submit(metricDescription.getName(), statusId, statusId, statusId);
        }
        bulkUpdater.done();
    }

    private void loadMetrics(int startTimestampSeconds) {
        log.info("Loading metric names from db");
        final AtomicInteger metricCount = new AtomicInteger();

        graphouseJdbcTemplate.query(
            "SELECT name, status FROM metric WHERE updated >= FROM_UNIXTIME(?)",
            new RowCallbackHandler() {
                @Override
                public void processRow(ResultSet rs) throws SQLException {
                    String metric = rs.getString("name");
                    MetricStatus status = MetricStatus.forId(rs.getInt("status"));
                    if (!metricValidator.validate(metric, true)) {
                        log.warn("Invalid metric in db: " + metric);
                        return;
                    }
                    metricTree.modify(metric, status);
                    metricCount.incrementAndGet();
                }
            },
            startTimestampSeconds
        );
        log.info("Loaded " + metricCount.get() + " metrics");
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
        loadMetrics(lastUpdatedTimestampSeconds);
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
        final StringBuilder metricBuilder = new StringBuilder();
        final AtomicInteger count = new AtomicInteger();

        metricTree.search(query, new Appendable() {
            @Override
            public Appendable append(CharSequence csq) {
                metricBuilder.append(csq);
                return this;
            }

            @Override
            public Appendable append(CharSequence csq, int start, int end) {
                metricBuilder.append(csq, start, end);
                return this;
            }

            @Override
            public Appendable append(char c) throws IOException {
                if (c == '\n') {
                    modify(metricBuilder.toString(), status);
                    if (result != null) {
                        result.append(metricBuilder).append('\n');
                    }
                    metricBuilder.setLength(0);
                    count.incrementAndGet();
                } else {
                    metricBuilder.append(c);
                }
                return this;
            }
        });
        return count.get();
    }

    public void modify(String metric, MetricStatus status) {
        modify(Collections.singletonList(metric), status);
    }


    public void modify(List<String> metrics, MetricStatus status) {
        if (metrics == null || metrics.isEmpty()) {
            return;
        }
        if (status.handmade()) {
            throw new IllegalStateException();
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
        metricTree.search(query, result);
    }

    @Required
    public void setGraphouseJdbcTemplate(JdbcTemplate graphouseJdbcTemplate) {
        this.graphouseJdbcTemplate = graphouseJdbcTemplate;
    }

    @Required
    public void setMonitoring(ComplicatedMonitoring monitoring) {
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

}
