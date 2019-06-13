package ru.yandex.market.graphouse.data;

import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.io.RuntimeIOException;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 16/11/2018
 */
public class MetricDataRowCallbackHandler implements RowCallbackHandler {

    private static final Logger log = LogManager.getLogger();

    private final JsonWriter jsonWriter;
    private final MetricDataQueryParams queryParams;
    private final Set<String> remainingMetrics;

    private String currentMetric = null;
    private int nextTs;


    public MetricDataRowCallbackHandler(JsonWriter jsonWriter, MetricDataQueryParams queryParams, Set<String> metrics) {
        this.jsonWriter = jsonWriter;
        this.remainingMetrics = metrics;
        this.queryParams = queryParams;
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
                nextTs = ts + queryParams.getStepSeconds();
            }
        } catch (IOException e) {
            log.error("Failed to read data from CH", e);
            throw new RuntimeIOException(e);
        }
    }

    public void finish() {
        try {
            endMetric();
            Iterator<String> remainingMetricIterator = remainingMetrics.iterator();
            while (remainingMetricIterator.hasNext()) {
                String remainingMetric = remainingMetricIterator.next();
                remainingMetricIterator.remove();
                startMetric(remainingMetric);
                endMetric();
            }
        } catch (IOException e) {
            log.error("Failed to read data from CH", e);
            throw new RuntimeIOException(e);
        }
    }

    private void fillNulls(int max) throws IOException {
        for (; nextTs < max; nextTs += queryParams.getStepSeconds()) {
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
        fillNulls(queryParams.getEndTimeSeconds());
        jsonWriter.endArray().endObject();
        currentMetric = null;
    }

    private void startMetric(String metric) throws IOException {
        remainingMetrics.remove(metric);
        nextTs = queryParams.getStartTimeSeconds();
        currentMetric = metric;
        jsonWriter.name(metric).beginObject();
        jsonWriter.name("start").value(queryParams.getStartTimeSeconds());
        jsonWriter.name("end").value(queryParams.getEndTimeSeconds());
        jsonWriter.name("step").value(queryParams.getStepSeconds());
        jsonWriter.name("points").beginArray();
    }
}
