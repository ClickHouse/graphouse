package ru.yandex.market.graphouse.data;

import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 09.11.16
 */
public class MetricDataResult {
    private final MetricDataParameters parameters;
    private final Set<String> metricsWithoutData;
    private final JsonWriter jsonWriter;

    private String previousMetric = null;
    private int previousPosition = 0;

    private int calcDataPosition(long kvantT) {
        return (int) (kvantT - parameters.getStartTimeSeconds()) / parameters.getMetricStep().getStepSizeInSeconds();
    }

    private void writeNulls(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            jsonWriter.nullValue();
        }
    }

    private void endPreviousData() throws IOException {
        if (previousMetric != null) {
            writeNulls(parameters.getPointsCount() - previousPosition - 1);
            jsonWriter.endArray();
        }
    }

    private void beginNewData(String metric) throws IOException {
        jsonWriter.name(metric).beginArray();
    }

    private void openNewMetricData(String metric) throws IOException {
        endPreviousData();
        beginNewData(metric);

        previousMetric = metric;
        previousPosition = 0;
        metricsWithoutData.remove(metric);
    }

    private void writeTimeInfo() throws IOException {
        jsonWriter.name("timeInfo")
            .beginArray()
            .value(parameters.getStartTimeSeconds())
            .value(parameters.getEndTimeSeconds())
            .value(parameters.getMetricStep().getStepSizeInSeconds())
            .endArray();
    }

    public MetricDataResult(MetricDataParameters parameters, Writer writer) throws IOException {
        this.parameters = parameters;
        this.metricsWithoutData = new HashSet<>(parameters.getMetrics());
        this.jsonWriter = new JsonWriter(writer).beginObject();

        writeTimeInfo();
        jsonWriter.name("data").beginObject();
    }

    public void appendData(String metric, long kvantT, float value) throws IOException {
        if ((parameters.isMultiMetrics() && !metric.equals(previousMetric)) || previousMetric == null) {
            openNewMetricData(metric);
        }

        final int position = calcDataPosition(kvantT);
        writeNulls(position - previousPosition - 1);
        jsonWriter.value(value);
        previousPosition = position;
    }

    public void appendData(long kvantT, float value) throws IOException {
        String metric = parameters.getFirstMetric();
        appendData(metric, kvantT, value);
    }

    public void flush() throws IOException {
        endPreviousData();
        for (String metric : metricsWithoutData) {
            beginNewData(metric);
            jsonWriter.endArray();
        }
        jsonWriter.endObject().endObject();
    }
}
