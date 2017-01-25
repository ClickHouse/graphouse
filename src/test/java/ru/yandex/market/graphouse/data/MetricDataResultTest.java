package ru.yandex.market.graphouse.data;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a> on 24.01.17
 */
public class MetricDataResultTest {
    private Map<String, float[]> metricsData;
    private int dataPointsCount;

    @Before
    public void initData() {
        dataPointsCount = 10;
        metricsData = new HashMap<>();
        metricsData.put("one_min.full_data_metric", new float[]{10, 20, 30, 40, 50, 60, 70, 80, 90, 100});
        metricsData.put("one_min.part_data_metric", new float[]{110, 120, -1, -1, 130, 140, -1, -1, 150, 160});
        metricsData.put("one_min.no_data_metric", new float[0]);

        final Optional<float[]> incorrectData = metricsData.values().stream()
            .filter(v -> v.length > 0 && v.length != dataPointsCount)
            .findFirst();

        if (incorrectData.isPresent()) {
            throw new IllegalArgumentException("Incorrect data " + Arrays.toString(incorrectData.get()));
        }
    }

    private void check(MetricDataParameters dataParameters, int stepSize, String expected) throws IOException {
        final StringWriter stringWriter = new StringWriter();
        final MetricDataResult result = new MetricDataResult(dataParameters, stringWriter);

        for (String metric : dataParameters.getMetrics()) {
            float[] metricData = metricsData.get(metric);
            if (metricData == null || metricData.length == 0) {
                continue;
            }

            for (int i = 0; i < dataParameters.getPointsCount(); i++) {
                float value = metricData[i];
                if (value != -1) {
                    int time = dataParameters.getStartTimeSeconds() + i * stepSize;
                    result.appendData(metric, time, value);
                }
            }
        }
        result.flush();

        final String metricDataJson = stringWriter.toString();
        Assert.assertEquals(expected, metricDataJson);

        final JsonObject data = new JsonParser().parse(metricDataJson).getAsJsonObject().getAsJsonObject("data");
        final boolean allSizesCorrect = data.entrySet().stream()
            .map(Map.Entry::getValue)
            .map(JsonElement::getAsJsonArray)
            .map(JsonArray::size)
            .allMatch(o -> o == dataParameters.getPointsCount());

        Assert.assertTrue(allSizesCorrect);
    }

    @Test
    public void testResult() throws IOException {
        final int metricsStep = MetricStep.ONE_MIN.getStepSizeInSeconds();
        final int startTimeSeconds = 0;
        final int endTimeSeconds = startTimeSeconds + 10 * metricsStep;

        final MetricDataParameters singleMetric = new MetricDataParameters(Collections.singletonList("one_min.full_data_metric"), startTimeSeconds, endTimeSeconds);
        final MetricDataParameters singleMetricWithShift = new MetricDataParameters(Collections.singletonList("one_min.full_data_metric"), startTimeSeconds + 10, endTimeSeconds + 10);
        final MetricDataParameters onePointMetric = new MetricDataParameters(Collections.singletonList("one_min.full_data_metric"), startTimeSeconds, startTimeSeconds + 60);
        final MetricDataParameters emptyMetric = new MetricDataParameters(Collections.singletonList("one_min.no_data_metric"), startTimeSeconds, endTimeSeconds);
        final MetricDataParameters allMetrics = new MetricDataParameters(Arrays.asList("one_min.full_data_metric", "one_min.no_data_metric", "one_min.part_data_metric"), startTimeSeconds, endTimeSeconds);

        check(singleMetric, metricsStep, "{\"timeInfo\":[0,600,60],\"data\":{\"one_min.full_data_metric\":[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0,90.0,100.0]}}");
        check(singleMetricWithShift, metricsStep, "{\"timeInfo\":[0,600,60],\"data\":{\"one_min.full_data_metric\":[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0,90.0,100.0]}}");
        check(onePointMetric, metricsStep, "{\"timeInfo\":[0,60,60],\"data\":{\"one_min.full_data_metric\":[10.0]}}");
        check(emptyMetric, metricsStep, "{\"timeInfo\":[0,600,60],\"data\":{\"one_min.no_data_metric\":[null,null,null,null,null,null,null,null,null,null]}}");
        check(allMetrics, metricsStep, "{\"timeInfo\":[0,600,60],\"data\":{\"one_min.full_data_metric\":[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0,90.0,100.0],\"one_min.part_data_metric\":[110.0,120.0,null,null,130.0,140.0,null,null,150.0,160.0],\"one_min.no_data_metric\":[null,null,null,null,null,null,null,null,null,null]}}");
    }
}