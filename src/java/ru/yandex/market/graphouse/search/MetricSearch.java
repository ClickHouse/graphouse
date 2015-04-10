package ru.yandex.market.graphouse.search;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.common.util.db.BulkUpdater;
import ru.yandex.market.clickhouse.ClickhouseTemplate;
import ru.yandex.market.clickhouse.HttpResultRow;
import ru.yandex.market.clickhouse.HttpRowCallbackHandler;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 07/04/15
 */
public class MetricSearch implements InitializingBean, Runnable {

    private static final Logger log = LogManager.getLogger();

    private ClickhouseTemplate clickhouseTemplate;
    private JdbcTemplate graphouseJdbcTemplate;
    private JdbcTemplate sqliteJdbcTemplate;

    private String sqliteDbFileName = "/var/lib/yandex/graphouse/logshatter.db";


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
        initDatabases();
        new Thread(this, "MetricSearch thread").start();
    }

    private void initDatabases() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + sqliteDbFileName);
        sqliteJdbcTemplate = new JdbcTemplate(dataSource);

        sqliteJdbcTemplate.update(
            "CREATE TABLE IF NOT EXISTS metrics (" +
                "name TEXT NOT NULL, " +
                "PRIMARY KEY (name))"
        );


        sqliteJdbcTemplate.update(
            "CREATE TABLE IF NOT EXISTS params (" +
                "name TEXT NOT NULL, " +
                "value INTEGER NOT NULL, " +
                "PRIMARY KEY (name))"
        );

        graphouseJdbcTemplate.update(
            "CREATE TABLE IF NOT EXISTS ban (" +
                "  `name` VARCHAR(255) NOT NULL, " +
                "  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                "  PRIMARY KEY (`name`), " +
                "  INDEX (`created`)" +
                ") "
        );
    }

    private int loadBans(int startTimestampSeconds) {
        final AtomicInteger count = new AtomicInteger();
        graphouseJdbcTemplate.query(
            "select name from ban where created > ?",
            new RowCallbackHandler() {
                @Override
                public void processRow(ResultSet rs) throws SQLException {
                    String metric = rs.getString("name");
                    metricTree.ban(metric);
                    count.incrementAndGet();
                }
            },
            startTimestampSeconds
        );
        return count.get();
    }

    private void saveNewMetrics() {
        if (!newMetricQueue.isEmpty()) {
            log.info("Saving new metric names to sqlite");
            int count = 0;
            BulkUpdater bulkUpdater = new BulkUpdater(
                sqliteJdbcTemplate,
                "INSERT OR IGNORE INTO metrics (name) values (?)",
                1000
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
        loadData();
        while (!Thread.interrupted()) {
            try {
                update();
            } catch (Exception e) {
                log.error("Failed to update metric search", e);
            }
            try {
                Thread.sleep(saveIntervalSeconds);
            } catch (InterruptedException ignored) {
            }

        }
    }

    private void loadData() {
        lastUpdatedTimestampSeconds = sqliteJdbcTemplate.queryForObject(
            "select IFNULL(max(value), -1) from params where name = 'updated'",
            Integer.class
        );

        log.info("Loading metric search data");
        int count = loadBans(0);
        log.info("Loaded " + count + " baned metrics");

        log.info("Loading metric names from sqlite");
        final AtomicInteger metricCount = new AtomicInteger();
        sqliteJdbcTemplate.query(
            "select name from metrics",
            new RowCallbackHandler() {
                @Override
                public void processRow(ResultSet rs) throws SQLException {
                    add(rs.getString("name"));
                    metricCount.incrementAndGet();
                }
            }
        );
        log.info("Loaded " + metricCount.get() + " metric name from sqlite");
    }

    private void update() {
        int startTimeSeconds = lastUpdatedTimestampSeconds;
        int endTimeSeconds = (int) (System.currentTimeMillis() / 1000) - updateDelaySeconds;

        log.info("Loading new bans");
        loadBans(startTimeSeconds);
        loadNamesFromClickHouse(startTimeSeconds, endTimeSeconds);

        saveNewMetrics();
        sqliteJdbcTemplate.update(
            "INSERT OR REPLACE INTO params (name, value), ('updated', ?)",
            lastUpdatedTimestampSeconds
        );


    }

    private void loadNamesFromClickHouse(int startTimeSeconds, int endTimeSeconds) {
        log.info("Fetching new metric names from Clickhouse");
        final AtomicInteger count = new AtomicInteger();
        clickhouseTemplate.query(
            "select distinct Path from graphite " +
                "where Timestamp > " + startTimeSeconds + " and Timestamp < " + endTimeSeconds,
            new HttpRowCallbackHandler() {
                @Override
                public void processRow(HttpResultRow rs) throws SQLException {
                    if (add(rs.getString("Path"))) {
                        count.incrementAndGet();
                    }
                }
            }
        );
        log.info("Found " + count.get() + " metric names");
    }

    public boolean add(String metric) {
        if (metricTree.add(metric)) {
            newMetricQueue.add(metric);
            return true;
        }
        return false;
    }

    public void ban(String metric) {
        graphouseJdbcTemplate.update(
            "INSERT IGNORE INTO ban (name), values(?)",
            metric
        );
        metricTree.ban(metric);
    }

    public void search(String query, Appendable result) throws IOException {
        metricTree.search(query, result);
    }


    @Required
    public void setClickhouseTemplate(ClickhouseTemplate clickhouseTemplate) {
        this.clickhouseTemplate = clickhouseTemplate;
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

    public void setSqliteDbFileName(String sqliteDbFileName) {
        this.sqliteDbFileName = sqliteDbFileName;
    }
}
