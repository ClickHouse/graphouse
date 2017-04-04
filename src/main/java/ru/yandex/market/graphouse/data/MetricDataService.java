package ru.yandex.market.graphouse.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.io.RuntimeIOException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.tree.MetricName;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/04/2017
 */
public class MetricDataService {

    private static final Logger log = LogManager.getLogger();

    private final MetricSearch metricSearch;

    private final JdbcTemplate clickHouseJdbcTemplate;

    private final String graphiteTable;

    public MetricDataService(MetricSearch metricSearch, JdbcTemplate clickHouseJdbcTemplate, String graphiteTable) {
        this.metricSearch = metricSearch;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.graphiteTable = graphiteTable;
    }


    public void getData(List<String> metricStrings, int startTimeSeconds, int endTimeSeconds,
                        PrintWriter writer) throws Exception {

        JsonWriter jsonWriter = new JsonWriter(writer);
        ListMultimap<String, MetricName> functionToMetrics = getFunctionToMetrics(metricStrings);


        jsonWriter.beginArray();
        for (String function : functionToMetrics.keySet()) {
            appendData(functionToMetrics.get(function), function, startTimeSeconds, endTimeSeconds, jsonWriter);
        }
        jsonWriter.endArray();
    }

    private void appendData(List<MetricName> metrics, String function,
                            int startTimeSeconds, int endTimeSeconds, JsonWriter jsonWriter) {

        int timeSeconds = endTimeSeconds - startTimeSeconds;
        int stepSeconds = selectStep(metrics, startTimeSeconds);


        int dataPoints = timeSeconds / stepSeconds;

        startTimeSeconds = startTimeSeconds / stepSeconds * stepSeconds;
        endTimeSeconds = startTimeSeconds + (dataPoints * stepSeconds);

        MetricDataRowCallbackHandler handler = new MetricDataRowCallbackHandler(
            jsonWriter, startTimeSeconds, endTimeSeconds, stepSeconds
        );
        clickHouseJdbcTemplate.query(
            createQuery(metrics, function, startTimeSeconds, endTimeSeconds, stepSeconds),
            handler
        );
        handler.finish();
    }

    @VisibleForTesting
    protected static class MetricDataRowCallbackHandler implements RowCallbackHandler {
        private final JsonWriter jsonWriter;
        private final int start;
        private final int end;
        private final int step;

        private String currentMetric = null;
        private int nextTs;

        public MetricDataRowCallbackHandler(JsonWriter jsonWriter, int start, int end, int step) {
            this.jsonWriter = jsonWriter;
            this.start = start;
            this.end = end;
            this.step = step;
            nextTs = start;
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            try {
                String metric = rs.getString("metric");
                int ts = rs.getInt("ts");
                double value = rs.getDouble("value");
                checkNewMetric(metric);
                fillNulls(ts);
                if (Double.isFinite(value)) {
                    jsonWriter.beginArray().value(value).value(ts).endArray();
                }
                nextTs = ts + step;
            } catch (IOException e) {
                log.error("Failed to read data from CH", e);
                throw new RuntimeIOException(e);
            }
        }

        public void finish() {
            try {
                endMetric();
            } catch (IOException e) {
                log.error("Failed to read data from CH", e);
                throw new RuntimeIOException(e);
            }
        }

        private void fillNulls(int max) throws IOException {
            for (; nextTs < max; nextTs += step) {
                jsonWriter.beginArray().nullValue().value(nextTs).endArray();
            }
        }

        private void checkNewMetric(String metric) throws IOException {
            if (metric.equals(currentMetric)) {
                return;
            }
            if (currentMetric != null) {
                endMetric();
            }
            startMetric(metric);
        }

        private void endMetric() throws IOException {
            fillNulls(end + 1);
            jsonWriter.endArray().endObject();
            currentMetric = null;
        }

        private void startMetric(String metric) throws IOException {
            nextTs = start;
            currentMetric = metric;
            jsonWriter.beginObject();
            jsonWriter.name("target").value(metric);
            jsonWriter.name("datapoints").beginArray();
        }


    }

    private String createQuery(List<MetricName> metrics, String function,
                               int startTimeSeconds, int endTimeSeconds, int stepSeconds) {

        StringBuilder builder = new StringBuilder();

        builder.append("SELECT metric, ts, ").append(function).append("(value) as value FROM (\n");

        builder.append("    SELECT metric, ts, argMax(value, updated) as value ");
        builder.append("FROM ").append(graphiteTable).append(" ");
        builder.append("WHERE metric IN (").append(toMetricString(metrics)).append(") ");
        builder.append("AND ts >= ").append(startTimeSeconds).append(" ");
        builder.append("AND ts < ").append(endTimeSeconds).append(" ");
        builder.append("AND date >= toDate(").append(startTimeSeconds).append(") ");
        builder.append("AND date <= toDate(").append(endTimeSeconds).append(") ");
        builder.append("GROUP BY metric, timestamp as ts\n");

        builder.append(") GROUP BY metric, ");
        builder.append("intDiv(toUInt32(ts), ").append(stepSeconds).append(") * ").append(stepSeconds).append(" as ts");
        builder.append(" ORDER BY metric, ts");
        return builder.toString();
    }

    private static String toMetricString(List<MetricName> metrics) {
        return metrics.stream().map(MetricName::getName).collect(Collectors.joining("','", "'", "'"));
    }


    private int selectStep(List<MetricName> metrics, int startTimeSeconds) {
        int ageSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - startTimeSeconds;
        int step = 1;
        for (MetricName metric : metrics) {
            step = Math.max(metric.getRetention().getStepSize(ageSeconds), step);
        }
        return step;
    }

    private ListMultimap<String, MetricName> getFunctionToMetrics(List<String> metricStrings) throws IOException {
        ListMultimap<String, MetricName> functionToMetrics = ArrayListMultimap.create();

        for (String metricString : metricStrings) {
            metricSearch.search(metricString, metric -> {
                if (metric instanceof MetricName) {
                    MetricName metricName = (MetricName) metric;
                    functionToMetrics.put(metricName.getRetention().getFunction(), metricName);
                }
            });
        }
        return functionToMetrics;
    }


}
