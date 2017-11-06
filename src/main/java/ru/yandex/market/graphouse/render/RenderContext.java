package ru.yandex.market.graphouse.render;

import ru.yandex.market.graphouse.search.MetricSearch;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 29/07/2017
 */
public class RenderContext {
    private final String table;
    private final MetricSearch metricSearch;
    private final int startTimeSeconds;
    private final int endTimeSeconds;
    private final int maxDataPoints;
    private final int maxMetricsPerQuery;

    public RenderContext(String table, MetricSearch metricSearch,
                         int startTimeSeconds, int endTimeSeconds, int maxDataPoints, int maxMetricsPerQuery) {
        this.table = table;
        this.metricSearch = metricSearch;
        this.startTimeSeconds = startTimeSeconds;
        this.endTimeSeconds = endTimeSeconds;
        this.maxDataPoints = maxDataPoints;
        this.maxMetricsPerQuery = maxMetricsPerQuery;
    }

    public String getTable() {
        return table;
    }

    public MetricSearch getMetricSearch() {
        return metricSearch;
    }

    public int getStartTimeSeconds() {
        return startTimeSeconds;
    }

    public int getEndTimeSeconds() {
        return endTimeSeconds;
    }

    public int getMaxDataPoints() {
        return maxDataPoints;
    }

    public int getMaxMetricsPerQuery() {
        return maxMetricsPerQuery;
    }
}
