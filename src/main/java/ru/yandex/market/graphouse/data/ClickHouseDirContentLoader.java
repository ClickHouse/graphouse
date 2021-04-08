package ru.yandex.market.graphouse.data;

import com.google.common.base.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.MetricDescription;
import ru.yandex.market.graphouse.statistics.LoadedMetricsCounter;

import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseDirContentLoader extends ClickHouseQueryExecutor {
    private static final Logger log = LogManager.getLogger();

    private final String metricsTable;
    private final LoadedMetricsCounter loadedMetricsCounter;

    public ClickHouseDirContentLoader(
        JdbcTemplate clickHouseJdbcTemplate, int queryRetryCount, int queryRetryIncrementSec,
        String metricsTable, LoadedMetricsCounter loadedMetricsCounter
    ) {
        super(clickHouseJdbcTemplate, queryRetryCount, queryRetryIncrementSec);
        this.metricsTable = metricsTable;
        this.loadedMetricsCounter = loadedMetricsCounter;
    }

    public void loadDirsContent(Set<? extends MetricDescription> dirs, DirContentRowCallbackHandler metricHandler) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        String dirFilter = dirs.stream().map(MetricDescription::getName).collect(Collectors.joining("','", "'", "'"));

        executeQuery(
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

        loadedMetricsCounter.addLoadedDirs(metricHandler.getDirCount());
        loadedMetricsCounter.addLoadedMetrics(metricHandler.getMetricCount());
    }

    public interface DirContentRowCallbackHandler extends RowCallbackHandler {
        int getMetricCount();

        int getDirCount();
    }
}
