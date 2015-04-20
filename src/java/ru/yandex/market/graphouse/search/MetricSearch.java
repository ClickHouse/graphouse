package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.common.util.db.BulkUpdater;
import ru.yandex.market.clickhouse.ClickhouseTemplate;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    private JdbcTemplate graphouseJdbcTemplate;

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
        new Thread(this, "MetricSearch thread").start();
    }

    private void initDatabase() {
        graphouseJdbcTemplate.update(
            "CREATE TABLE IF NOT EXISTS metric (" +
                "  `name` VARCHAR(200) NOT NULL, " +
                "  `ban` BIT NOT NULL DEFAULT 0, " +
                "  `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                "  PRIMARY KEY (`name`), " +
                "  INDEX (`updated`)" +
                ") "
        );
    }

    private void loadMetrics(int startTimestampSeconds) {
        log.info("Loading metric names from db");
        final AtomicInteger metricCount = new AtomicInteger();
        final AtomicInteger banCount = new AtomicInteger();

        graphouseJdbcTemplate.query(
            "SELECT name, ban FROM metric WHERE updated >= ?",
            new RowCallbackHandler() {
                @Override
                public void processRow(ResultSet rs) throws SQLException {
                    String metric = rs.getString("name");
                    boolean ban = rs.getBoolean("ban");
                    if (ban) {
                        metricTree.ban(metric);
                        banCount.incrementAndGet();
                    } else {
                        metricTree.add(metric);
                        metricCount.incrementAndGet();
                    }
                }
            },
            startTimestampSeconds
        );
        log.info("Loaded " + metricCount.get() + " metrics and " + banCount.get() + " bans");
    }

    private void saveNewMetrics() {
        if (!newMetricQueue.isEmpty()) {
            log.info("Saving new metric names to db");
            int count = 0;
            BulkUpdater bulkUpdater = new BulkUpdater(
                graphouseJdbcTemplate,
                "INSERT IGNORE INTO metric (name) values (?)",
                100000
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
                update();
                saveNewMetrics();
            } catch (Exception e) {
                log.error("Failed to update metric search", e);
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(saveIntervalSeconds));
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void update() {
        int timeSeconds = (int) (System.currentTimeMillis() / 1000) - updateDelaySeconds;
        loadMetrics(lastUpdatedTimestampSeconds);
        lastUpdatedTimestampSeconds = timeSeconds;
    }


    public MetricStatus add(String metric) {
        MetricStatus status = metricTree.add(metric);
        if (status == MetricStatus.NEW) {
            newMetricQueue.add(metric);
        }
        return status;
    }

    public void ban(String metric) {
        graphouseJdbcTemplate.update(
            "INSERT INTO metric (name, ban) VALUES (?, 1) " +
                "ON DUPLICATE KEY UPDATE ban = 1, updated = CURRENT_TIMESTAMP",
            metric
        );
        metricTree.ban(metric);
    }

    public void search(String query, Appendable result) throws IOException {
        metricTree.search(query, result);
    }

    @Required
    public void setGraphouseJdbcTemplate(JdbcTemplate graphouseJdbcTemplate) {
        this.graphouseJdbcTemplate = graphouseJdbcTemplate;
    }

    public void setSaveIntervalSeconds(int saveIntervalSeconds) {
        this.saveIntervalSeconds = saveIntervalSeconds;
    }

    public void setUpdateDelaySeconds(int updateDelaySeconds) {
        this.updateDelaySeconds = updateDelaySeconds;
    }

}
