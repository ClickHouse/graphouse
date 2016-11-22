package ru.yandex.market.graphouse.data;

import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 09.11.16
 */
public class MetricDataResult {
    private final Map<String, MetricDataService.GroupParameters> parametersMap;
    private final Set<String> metricsWithoutData;
    private final JsonWriter jsonWriter;

    private String previousMetric = null;
    private int previousPosition = 0;
    private MetricDataService.GroupParameters  parameters;

    private int calcDataPosition(long kvantT) {
        return (int) (kvantT - parameters.getStartTimeSeconds()) / parameters.getStepSizeInSeconds();
    }

    private void writeNulls(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            jsonWriter.nullValue();
        }
    }

    private void endPreviousData() throws IOException {
        if (previousMetric != null) {
            writeNulls(parameters.getPointsCount() - previousPosition - 1);
            jsonWriter.endArray().endObject();
        }
    }

    private void beginNewData(String metric) throws IOException {
        jsonWriter.name(metric).beginObject();
        writeTimeInfo();
        jsonWriter.name("data").beginArray();
    }

    private void openNewMetricData(String metric) throws IOException {
        endPreviousData();

        previousMetric = metric;
        previousPosition = 0;
        metricsWithoutData.remove(metric);
        parameters = parametersMap.get(metric);

        beginNewData(metric);
    }

    private void writeTimeInfo() throws IOException {
        jsonWriter.name("timeInfo")
            .beginArray()
            .value(parameters.getStartTimeSeconds())
            .value(parameters.getEndTimeSeconds())
            .value(parameters.getStepSizeInSeconds())
            .endArray();
    }

    MetricDataResult(Writer writer, Map<String, MetricDataService.GroupParameters> parametersMap) throws IOException {
        this.parametersMap = parametersMap;
        this.metricsWithoutData = new HashSet<>(parametersMap.keySet());
        this.jsonWriter = new JsonWriter(writer).beginObject();
    }

    void appendData(String metric, long quantT, float value) throws IOException {
        if (!metric.equals(previousMetric)) {
            openNewMetricData(metric);
        }

        final int position = calcDataPosition(quantT);
        writeNulls(position - previousPosition - 1);
        jsonWriter.value(value);
        previousPosition = position;
    }

    void flush() throws IOException {
        endPreviousData();
        for (String metric : metricsWithoutData) {
            beginNewData(metric);
            jsonWriter.endArray().endObject();
        }
        jsonWriter.endObject();
    }
}
