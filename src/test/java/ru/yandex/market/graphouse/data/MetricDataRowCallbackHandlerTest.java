package ru.yandex.market.graphouse.data;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;
import com.mockrunner.mock.jdbc.MockResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/04/2017
 */
public class MetricDataRowCallbackHandlerTest {

    @Test
    public void testHandler() throws Exception {

        MockResultSet resultSet = new MockResultSet("data");
        resultSet.addColumn("metric", new String[]{"name1", "name1", "name2", "name2"});
        resultSet.addColumn("ts", new Integer[]{100, 160, 160, 220});
        resultSet.addColumn("value", new Double[]{33.33, 42.0, 32.0, 77.7});

        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter);
        jsonWriter.beginObject();

        MetricDataService.MetricDataRowCallbackHandler handler = new MetricDataService.MetricDataRowCallbackHandler(
            jsonWriter, 100, 280, 60
        );

        while (resultSet.next()) {
            handler.processRow(resultSet);
        }
        handler.finish();

        jsonWriter.endObject();

        JsonObject expected = new JsonObject();
        expected.add("name1", createMetric(100, 280, 60, 33.33, 42.0, Double.NaN));
        expected.add("name2", createMetric(100, 280, 60, Double.NaN, 32.0, 77.7));

        Assert.assertEquals(expected.toString(), stringWriter.toString());
    }

    @Test
    public void testChNan() throws Exception {

        MockResultSet resultSet = new MockResultSet("data");
        resultSet.addColumn("metric", new String[]{"name1", "name1", "name1"});
        resultSet.addColumn("ts", new Integer[]{0, 1, 2});
        resultSet.addColumn("value", new Double[]{0.0, Double.NaN, 2.0});

        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter);
        jsonWriter.beginObject();

        MetricDataService.MetricDataRowCallbackHandler handler = new MetricDataService.MetricDataRowCallbackHandler(
            jsonWriter, 0, 3, 1
        );

        while (resultSet.next()) {
            handler.processRow(resultSet);
        }
        handler.finish();

        jsonWriter.endObject();

        JsonObject expected = new JsonObject();
        expected.add("name1", createMetric(0, 3, 1, 0.0, Double.NaN, 2.0));

        Assert.assertEquals(expected.toString(), stringWriter.toString());
    }


    private JsonObject createMetric(int start, int end, int step, double... values) {
        JsonObject metric = new JsonObject();
        metric.addProperty("start", start);
        metric.addProperty("end", end);
        metric.addProperty("step", step);
        metric.add("points", createPoints(values));
        return metric;
    }

    private JsonArray createPoints(double... values) {
        JsonArray points = new JsonArray();
        for (double value : values) {
            if (Double.isFinite(value)) {
                points.add(new JsonPrimitive(value));
            } else {
                points.add(JsonNull.INSTANCE);
            }
        }
        return points;
    }


}