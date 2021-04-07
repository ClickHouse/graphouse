package ru.yandex.market.graphouse.save;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.search.tree.MetricDescription;
import ru.yandex.market.graphouse.statistics.AccumulatedMetric;
import ru.yandex.market.graphouse.statistics.StatisticsService;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class UpdateMetricQueueService {
    private static final Logger log = LogManager.getLogger();
    private static final int BATCH_SIZE = 5_000;
    private static final int MAX_METRICS_PER_SAVE = 1_000_000;

    private final StatisticsService statisticsService;
    private final String metricsTable;
    private final JdbcTemplate clickHouseJdbcTemplate;
    private final Queue<MetricDescription> updateQueue;

    public UpdateMetricQueueService(
        StatisticsService statisticsService, String metricsTable, JdbcTemplate clickHouseJdbcTemplate
    ) {
        this.statisticsService = statisticsService;
        this.metricsTable = metricsTable;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.updateQueue = new ConcurrentLinkedQueue<>();
    }

    public void addUpdatedMetrics(long startTimeMillis, MetricDescription metricDescription) {
        addUpdatedMetrics(startTimeMillis, metricDescription, updateQueue);
    }

    private void addUpdatedMetrics(long startTimeMillis, MetricDescription metricDescription,
                                   Collection<MetricDescription> updatedCollection) {
        if (metricDescription != null && metricDescription.getUpdateTimeMillis() >= startTimeMillis) {
            updatedCollection.add(metricDescription);
            addUpdatedMetrics(startTimeMillis, metricDescription.getParent(), updatedCollection);
        }
    }

    public void addUpdatedMetrics(MetricDescription metricDescription) {
        updateQueue.add(metricDescription);
    }

    public long saveUpdatedMetrics() {
        long startTimestampMs = System.currentTimeMillis();

        if (updateQueue.isEmpty()) {
            log.info("No new metric names to save");
            return System.currentTimeMillis() - startTimestampMs;
        }
        log.info("Saving new metric names to db. Current count: " + updateQueue.size());
        List<MetricDescription> metrics = new ArrayList<>();
        MetricDescription metric;
        while (metrics.size() <= MAX_METRICS_PER_SAVE && (metric = updateQueue.poll()) != null) {
            metrics.add(metric);
        }
        try {
            saveMetrics(metrics);
            statisticsService.accumulateMetric(
                AccumulatedMetric.NUMBER_OF_UPDATED_METRIC_TREE_NODES, metrics.size()
            );
            log.info("Saved " + metrics.size() + " metric names");
        } catch (Exception e) {
            log.error("Failed to save metrics to database", e);
            updateQueue.addAll(metrics);
        }

        return System.currentTimeMillis() - startTimestampMs;
    }

    public void saveMetrics(List<MetricDescription> metrics) {
        if (metrics.isEmpty()) {
            return;
        }

        final String sql = "INSERT INTO " + metricsTable
            + " (name, level, parent, status, updated) VALUES (?, ?, ?, ?, ?)";

        final int batchesCount = (metrics.size() - 1) / BATCH_SIZE + 1;

        for (int batchNum = 0; batchNum < batchesCount; batchNum++) {

            int firstIndex = batchNum * BATCH_SIZE;
            int lastIndex = firstIndex + BATCH_SIZE;

            lastIndex = Math.min(lastIndex, metrics.size());
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
}
