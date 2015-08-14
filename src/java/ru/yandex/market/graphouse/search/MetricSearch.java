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

    private JdbcTemplate graphouseJdbcTemplate;
    private ComplicatedMonitoring monitoring;
    private MetricValidator metricValidator;

    private MonitoringUnit metricSearchUnit = new MonitoringUnit("MetricSearch");
    private final MetricTree metricTree = new MetricTree();
    private final Queue<String> newMetricQueue = new ConcurrentLinkedQueue<>();

    private int lastUpdatedTimestampSeconds = 0;

    private int saveIntervalSeconds = 300;
    /**
     * Задержка на запись, репликацию, синхронизацию
     */
    private int updateDelaySeconds = 120;

    @Override
    public void afterPropertiesSet() throws Exception {
        initDatabase();
        monitoring.addUnit(metricSearchUnit);
        new Thread(this, "MetricSearch thread").start();
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

    private void loadMetrics(int startTimestampSeconds) {
        log.info("Loading metric names from db");
        final AtomicInteger metricCount = new AtomicInteger();

        graphouseJdbcTemplate.query(
            "SELECT name, status FROM metric WHERE updated >= ?",
            new RowCallbackHandler() {
                @Override
                public void processRow(ResultSet rs) throws SQLException {
                    String metric = rs.getString("name");
                    MetricStatus status = MetricStatus.forId(rs.getInt("status"));
                    if (!metricValidator.validate(metric, true)) {
                        log.warn("Invalid metric in db: " + metric);
                        return;
                    }
                    metricTree.add(metric, status);
                    metricCount.incrementAndGet();
                }
            },
            startTimestampSeconds
        );
        log.info("Loaded " + metricCount.get() + " metrics");
    }

    private void saveNewMetrics() {
        if (!newMetricQueue.isEmpty()) {
            log.info("Saving new metric names to db");
            int count = 0;
            BulkUpdater bulkUpdater = new BulkUpdater(
                graphouseJdbcTemplate,
                "INSERT IGNORE INTO metric (name) values (?)",
                BATCH_SIZE
            );
            String metric;
            while ((metric = newMetricQueue.poll()) != null) {
                bulkUpdater.submit(metric);
                count++;
            }
            bulkUpdater.done();
            log.info("Saved " + count + " metric names");
        } else {
            log.info("No new metric names to save");
        }
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                loadNewMetrics();
                saveNewMetrics();
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
        int timeSeconds = (int) (System.currentTimeMillis() / 1000) - updateDelaySeconds;
        loadMetrics(lastUpdatedTimestampSeconds);
        lastUpdatedTimestampSeconds = timeSeconds;
    }

    public QueryStatus add(String metric) {
        QueryStatus status = metricTree.add(metric);
        if (status == QueryStatus.NEW) {
            newMetricQueue.add(metric);
        }
        return status;
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
        if (status.equals(MetricStatus.SIMPLE)) {
            throw new IllegalStateException();
        }
        BulkUpdater bulkUpdater = new BulkUpdater(
            graphouseJdbcTemplate,
            "INSERT INTO metric (name, status) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE status = ?, updated = CURRENT_TIMESTAMP",
            BATCH_SIZE
        );
        for (String metric : metrics) {
            if (!metricValidator.validate(metric, true)) {
                log.warn("Wrong metric to modify: " + metric);
                continue;
            }
            bulkUpdater.submit(metric, status.getId(), status.getId());
            metricTree.add(metric, status);

        }
        bulkUpdater.done();
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
