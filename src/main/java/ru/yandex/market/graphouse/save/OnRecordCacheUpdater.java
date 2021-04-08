package ru.yandex.market.graphouse.save;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.monitoring.MonitoringUnit;
import ru.yandex.market.graphouse.save.banned.BannedMetricCache;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OnRecordCacheUpdater implements InitializingBean, Runnable {
    private static final Logger log = LogManager.getLogger();

    private final OnRecordMetricProvider metricProvider;
    private final BannedMetricCache metricCache;
    private final boolean cacheEnable;

    private boolean banCacheInitialized = false;
    private final MonitoringUnit cacheInitMonitoring;
    private final MonitoringUnit cacheUpdateUnit;
    private final int maxLevel;
    private final JdbcTemplate clickHouseJdbcTemplate;
    private final String metricsTable;
    private final int refreshSeconds;
    private final int queryRetryCount;
    private final long maxBatchSize;

    public OnRecordCacheUpdater(
        OnRecordMetricProvider metricProvider,
        BannedMetricCache metricCache,
        boolean cacheEnable,
        Monitoring ping,
        Monitoring monitoring,
        int maxLevel,
        JdbcTemplate clickHouseJdbcTemplate,
        String metricsTable,
        int refreshSeconds,
        int queryRetryCount,
        long maxBatchSize
    ) {
        this.metricProvider = metricProvider;
        this.metricCache = metricCache;
        this.cacheEnable = cacheEnable;
        this.maxLevel = maxLevel;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.metricsTable = metricsTable;
        this.refreshSeconds = refreshSeconds;
        this.queryRetryCount = queryRetryCount;
        this.maxBatchSize = maxBatchSize;

        this.cacheInitMonitoring = new MonitoringUnit("InitBannedMetricCache");
        if (cacheEnable) {
            cacheInitMonitoring.critical("OnRecordCacheUpdater is not initialized");
        }
        ping.addUnit(cacheInitMonitoring);

        this.cacheUpdateUnit = new MonitoringUnit("UpdateBannedMetricCache");
        monitoring.addUnit(cacheUpdateUnit);
    }

    public boolean isMetricCacheInitialized() {
        return banCacheInitialized;
    }

    public MonitoringUnit getCacheInitMonitoring() {
        return cacheInitMonitoring;
    }

    public boolean isOnRecordCacheDisabled() {
        return !cacheEnable;
    }

    @Override
    public void afterPropertiesSet() {
        if (cacheEnable) {
            new Thread(this, "OnRecordCacheUpdater thread").start();
        } else {
            log.info("OnRecordCacheUpdater disabled");
        }
    }

    @Override
    public void run() {
        log.info("OnRecordCacheUpdater thread started");
        while (!Thread.interrupted() && !isMetricCacheInitialized()) {
            try {
                log.info(metricCache.printCacheState());
                initMetricCache();
            } catch (Exception e) {
                log.error("Unexpected error on update banned cache", e);
            }

            try {
                TimeUnit.SECONDS.sleep(refreshSeconds);
            } catch (InterruptedException ignored) {
            }
        }
        log.info(metricCache.printCacheState());
        log.info("OnRecordCacheUpdater thread finished");
    }

    private void initMetricCache() {
        long startTime = System.currentTimeMillis();
        try {
            initBanCache();
            cacheInitMonitoring.ok();
            cacheUpdateUnit.ok();
        } catch (Exception e) {
            log.error("Error on update BannedMetricCache", e);
            if (isMetricCacheInitialized()) {
                cacheUpdateUnit.warning("Error on update BannedMetricCache", e);
            } else {
                cacheUpdateUnit.critical("Error on init BannedMetricCache", e);
            }
        } finally {
            log.info("Initialized metric cache in {} ms", System.currentTimeMillis() - startTime);
        }
    }

    private void initBanCache() {
        if (banCacheInitialized) {
            return;
        }
        log.info("Init banned cache...");

        MetricStatusHandler metricStatusHandler = new MetricStatusHandler();

        for (int level = 1; level <= maxLevel; level++) {
            boolean levelLoaded = loadBannedMetricsForLevel(metricStatusHandler, level);
            if (!levelLoaded) {
                String errorMessage = "Can't load banned metrics on level: " + level;
                log.error(errorMessage);
                throw new RuntimeException(errorMessage);
            }
        }
        logSuccessfulLoad(metricStatusHandler);

        log.info("Banned cache initialized");
        banCacheInitialized = true;
    }

    private boolean loadBannedMetricsForLevel(MetricStatusHandler handler, int level) {
        long offset = 0;
        boolean loaded;

        do {
            handler.reset();
            boolean batchLoaded = loadBatchOfBannedMetricsForLevel(handler, level, offset);
            if (!batchLoaded) {
                return false;
            }
            loaded = handler.getLoadedStatesInQuery() == 0;
            offset += maxBatchSize;
        } while (!loaded);

        return true;
    }

    private boolean loadBatchOfBannedMetricsForLevel(RowCallbackHandler handler, int level, long offset) {
        String dirsWithBanStatesQuery = "SELECT" +
            "     name," +
            "     argMax(status, updated) AS last_status" +
            " FROM " + metricsTable +
            " WHERE level = ? AND name IN (" +
            "     SELECT name" +
            "     FROM " + metricsTable +
            "     WHERE level = ? AND status = ?" +
            "     ORDER BY name" +
            "     LIMIT ? OFFSET ?" +
            " )" +
            " GROUP BY name" +
            " HAVING last_status = ?";
        Object[] args = new Object[]{
            level,
            level,
            MetricStatus.BAN.name(),
            maxBatchSize,
            offset,
            MetricStatus.BAN.name()
        };

        return executeQuery(dirsWithBanStatesQuery, handler, args);
    }

    private boolean executeQuery(String sql, RowCallbackHandler handler, Object... args) {
        int attempts = 0;
        int waitTime = 1;
        while (!tryExecuteQuery(sql, handler, args)) {
            try {
                attempts++;
                if (attempts >= queryRetryCount) {
                    return false;
                } else {
                    log.warn(
                        "Failed to execute query. Attempt number {} / {}. Waiting {} second before retry",
                        attempts, queryRetryCount, waitTime
                    );
                    TimeUnit.SECONDS.sleep(waitTime);
                }
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }

    private boolean tryExecuteQuery(String sql, RowCallbackHandler handler, Object... args) {
        try {
            clickHouseJdbcTemplate.query(sql, handler, args);
        } catch (RuntimeException e) {
            log.error("Failed to execute query", e);
            return false;
        }
        return true;
    }

    private void logSuccessfulLoad(MetricStatusHandler metricStatusHandler) {
        log.info(
            "Loaded complete: {}",
            metricStatusHandler.getLoadedStates()
        );
    }

    public void tryToUpdateMetricCache(String fullName, MetricStatus status) {
        if (isOnRecordCacheDisabled() || !isMetricCacheInitialized()) {
            return;
        }
        initMetricCache(fullName, status);
    }

    private void initMetricCache(String fullName, MetricStatus status) {
        if (!cacheEnable) {
            return;
        }

        switch (status) {
            case BAN:
                metricCache.addMetricWithStatus(fullName, MetricStatus.BAN);
                metricProvider.removeMetricFromTree(fullName);
                break;
            case AUTO_HIDDEN:
            case HIDDEN:
            case APPROVED:
            case SIMPLE:
                metricCache.resetBanStatus(fullName, status);
                metricProvider.updateMetricIfLoaded(fullName, status);
                break;
            default:
                log.warn("Undefined status '{}' for metric '{}'", status, fullName);
        }
    }

    private class MetricStatusHandler implements RowCallbackHandler {
        private final Map<MetricStatus, Integer> loadedStates = new HashMap<>();
        private long loadedStatesInQuery = 0;

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            String fullName = rs.getString("name");
            MetricStatus status = MetricStatus.valueOf(rs.getString("last_status"));
            initMetricCache(fullName, status);

            loadedStates.compute(status, (s, v) -> v == null ? 1 : ++v);
            ++loadedStatesInQuery;
        }

        public Map<MetricStatus, Integer> getLoadedStates() {
            return loadedStates;
        }

        public long getLoadedStatesInQuery() {
            return loadedStatesInQuery;
        }

        public void reset() {
            loadedStatesInQuery = 0;
        }
    }
}
