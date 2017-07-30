package ru.yandex.market.graphouse.render;

import com.google.common.base.Preconditions;
import ru.yandex.market.graphouse.search.tree.MetricName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 17/07/2017
 */
public class QueryDataPointsProvider implements DataPointsProvider {
    private final String query;
    private final RenderContext context;
//    private final DataPointsParams params;
//    private final String aggregationFunction;


    public QueryDataPointsProvider(String query, RenderContext context) {
        this.query = query;
        this.context = context;
    }

    //TODO
    public DataPointsParams getParams(List<MetricName> metrics) {
        int timeSeconds = context.getEndTimeSeconds() - context.getStartTimeSeconds();
        int step = 60; //TODO select step
        int dataPoints = timeSeconds / step;
        if (context.getMaxDataPoints() > 0 && dataPoints < context.getMaxDataPoints()) {
            int valuesPerPoint = (int) Math.ceil((double) dataPoints / context.getMaxDataPoints());
            step *= valuesPerPoint;
            dataPoints = timeSeconds / step;
        }
        int startTimeSeconds = (context.getEndTimeSeconds() + 1) - (step * dataPoints);
        return new DataPointsParams(startTimeSeconds, step, dataPoints);
    }

    @Override
    public DataPoints getDataPoints(DataPointsParams params) {
        List<MetricName> metrics = getMetrics();
        //TODO agr func
        String clickhouseQuery = buildQuery(context.getTable(), metrics, params, "avg");
        return new DataPoints("metric", clickhouseQuery, params);
    }

    public List<MetricName> getMetrics() {
        List<MetricName> metrics = new ArrayList<>();
        try {
            context.getMetricSearch().search(
                query, metric -> {
                    if (!metric.isDir()) {
                        metrics.add((MetricName) metric);
                        Preconditions.checkArgument(
                            context.getMaxMetricsPerQuery() > 0 && metrics.size() > context.getMaxMetricsPerQuery(),
                            "More than %d metrics found for query: %s",
                            context.getMaxMetricsPerQuery(), query
                        );

                    }
                }
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return metrics;
    }


    public static String buildQuery(String table, List<MetricName> metrics,
                                    DataPointsParams params, String aggregationFunction) {

        return String.format(
            "SELECT metrics, groupArrayInsertAt(inf, %d)(value, (ts - %d) / %d) AS values FROM (" +
                "   SELECT metric, ts, %s(value) as value FROM (" +
                "       SELECT metric, ts, argMax(value, updated) as value FROM %s " +
                "           WHERE ts >=%d AND ts < %d AND date >=toDate(%d) AND date <= toDate(%d) " +
                "               AND metric IN (%s) " +
                "       GROUP BY metric, timestamp AS ts" +
                "   ) GROUP BY metric, intDiv(toUInt32(ts), %d) * %d AS ts" +
                ") ORDER BY ts",
            params.getPointsCount(), params.getStartTimeSeconds(), params.getStepSeconds(),
            aggregationFunction, table,
            params.getStartTimeSeconds(), params.getEndTimeSeconds(),
            params.getStartTimeSeconds(), params.getEndTimeSeconds(),
            metrics.stream().map(MetricName::getName).collect(Collectors.joining(",")),
            params.getStepSeconds(), params.getStepSeconds()
        );
    }


    @Override
    public DataPointsParams getDataPointsParams() {
        return getParams(getMetrics()); //TODO
    }
}
