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
        jsonWriter.beginArray();

        MetricDataService.MetricDataRowCallbackHandler handler = new MetricDataService.MetricDataRowCallbackHandler(
            jsonWriter, 100, 221, 60
        );

        while (resultSet.next()) {
            handler.processRow(resultSet);
        }
        handler.finish();

        jsonWriter.endArray();


        Assert.assertEquals(getExpected().toString(), stringWriter.toString());

        System.out.println();


    }

    private JsonArray getExpected() {
        JsonArray expected = new JsonArray();
        JsonObject metric1 = new JsonObject();
        metric1.addProperty("target", "name1");
        JsonArray datapoints1 = new JsonArray();
        addPoint(datapoints1, 100, 33.33);
        addPoint(datapoints1, 160, 42.0);
        addPoint(datapoints1, 220, Double.NaN);
        metric1.add("datapoints", datapoints1);
        expected.add(metric1);


        JsonObject metric2 = new JsonObject();
        metric2.addProperty("target", "name2");
        JsonArray datapoints2 = new JsonArray();
        addPoint(datapoints2, 100, Double.NaN);
        addPoint(datapoints2, 160, 32.0);
        addPoint(datapoints2, 220, 77.7);
        metric2.add("datapoints", datapoints2);
        expected.add(metric2);

        return expected;


    }

    private void addPoint(JsonArray datapoints, int ts, double value) {
        JsonArray point = new JsonArray();
        if (Double.isFinite(value)) {
            point.add(new JsonPrimitive(value));
        } else {
            point.add(JsonNull.INSTANCE);
        }
        point.add(new JsonPrimitive(ts));
        datapoints.add(point);
    }

}