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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    private final String graphiteDataReadTable;

    public MetricDataService(MetricSearch metricSearch, JdbcTemplate clickHouseJdbcTemplate,
                             String graphiteDataReadTable) {
        this.metricSearch = metricSearch;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.graphiteDataReadTable = graphiteDataReadTable;
    }


    public void getData(List<String> metricStrings, int startTimeSeconds, int endTimeSeconds,
                        PrintWriter writer) throws Exception {

        JsonWriter jsonWriter = new JsonWriter(writer);
        ListMultimap<String, MetricName> functionToMetrics = getFunctionToMetrics(metricStrings);


        jsonWriter.beginObject();
        for (String function : functionToMetrics.keySet()) {
            appendData(functionToMetrics.get(function), function, startTimeSeconds, endTimeSeconds, jsonWriter);
        }
        jsonWriter.endObject();
    }

    private void appendData(List<MetricName> metrics, String function,
                            int startTimeSeconds, int endTimeSeconds, JsonWriter jsonWriter) {

        int timeSeconds = endTimeSeconds - startTimeSeconds;
        int stepSeconds = selectStep(metrics, startTimeSeconds);


        int dataPoints = timeSeconds / stepSeconds;

        startTimeSeconds = startTimeSeconds / stepSeconds * stepSeconds;
        endTimeSeconds = startTimeSeconds + (dataPoints * stepSeconds);

        Set<String> metricsSet = new HashSet<>();
        String metricString = metrics.stream()
            .map(MetricName::getName)
            .peek(metricsSet::add)
            .collect(Collectors.joining("','", "'", "'"));


        MetricDataRowCallbackHandler handler = new MetricDataRowCallbackHandler(
            jsonWriter, startTimeSeconds, endTimeSeconds, stepSeconds, metricsSet
        );

        clickHouseJdbcTemplate.query(
            "SELECT metric, ts, " + function + "(value) as value FROM (" +
                "   SELECT metric, ts, argMax(value, updated) as value FROM " + graphiteDataReadTable +
                "       WHERE metric IN (" + metricString + ")" +
                "           AND ts >= ? AND ts < ? AND date >= toDate(?) AND date <= toDate(?)" +
                "       GROUP BY metric, timestamp as ts" +
                ") GROUP BY metric, intDiv(toUInt32(ts), ?) * ? as ts ORDER BY metric, ts",
            handler,
            startTimeSeconds, endTimeSeconds, startTimeSeconds, endTimeSeconds, stepSeconds, stepSeconds
        );
        handler.finish();
    }

    @VisibleForTesting
    protected static class MetricDataRowCallbackHandler implements RowCallbackHandler {
        private final JsonWriter jsonWriter;
        private final int start;
        private final int end;
        private final int step;
        private final Set<String> remainingMetrics;

        private String currentMetric = null;
        private int nextTs;


        public MetricDataRowCallbackHandler(JsonWriter jsonWriter, int start, int end, int step, Set<String> metrics) {
            this.jsonWriter = jsonWriter;
            this.start = start;
            this.end = end;
            this.step = step;
            this.remainingMetrics = metrics;
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
                    jsonWriter.value(value);
                    nextTs = ts + step;
                }
            } catch (IOException e) {
                log.error("Failed to read data from CH", e);
                throw new RuntimeIOException(e);
            }
        }

        public void finish() {
            try {
                endMetric();
                for (String remainingMetric : remainingMetrics) {
                    startMetric(remainingMetric);
                    endMetric();
                }
            } catch (IOException e) {
                log.error("Failed to read data from CH", e);
                throw new RuntimeIOException(e);
            }
        }

        private void fillNulls(int max) throws IOException {
            for (; nextTs < max; nextTs += step) {
                jsonWriter.nullValue();
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
            if (currentMetric == null) {
                return;
            }
            fillNulls(end);
            jsonWriter.endArray().endObject();
            currentMetric = null;
        }

        private void startMetric(String metric) throws IOException {
            remainingMetrics.remove(metric);
            nextTs = start;
            currentMetric = metric;
            jsonWriter.name(metric).beginObject();
            jsonWriter.name("start").value(start);
            jsonWriter.name("end").value(end);
            jsonWriter.name("step").value(step);
            jsonWriter.name("points").beginArray();
        }
    }

    private int selectStep(List<MetricName> metrics, int startTimeSeconds) {
        int ageSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - startTimeSeconds;
        return metrics.stream()
            .mapToInt(m -> m.getRetention().getStepSize(ageSeconds))
            .max()
            .orElse(1);
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
