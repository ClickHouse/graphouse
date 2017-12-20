package ru.yandex.market.graphouse.render;

import com.google.common.base.Stopwatch;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.io.RuntimeIOException;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.search.MetricSearch;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 18/02/2017
 */
public class RenderService {

    private static final Logger log = LogManager.getLogger();

    private final MetricSearch metricSearch;
    private final FunctionProcessor functionProcessor;
    private final JdbcTemplate jdbcTemplate;
    private final String dataTable;
    private final int maxMetricsPerQuery;

    public RenderService(MetricSearch metricSearch, FunctionProcessor functionProcessor,
                         JdbcTemplate jdbcTemplate, String dataTable, int maxMetricsPerQuery) {
        this.metricSearch = metricSearch;
        this.functionProcessor = functionProcessor;
        this.jdbcTemplate = jdbcTemplate;
        this.dataTable = dataTable;
        this.maxMetricsPerQuery = maxMetricsPerQuery;
    }

    public void render(int startTimeSeconds, int endTimeSeconds, String target, int maxDataPoints,
                       PrintWriter writer) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        RenderContext context = new RenderContext(
            dataTable, metricSearch, startTimeSeconds, endTimeSeconds, maxDataPoints, maxMetricsPerQuery
        );

        DataPointsProvider dataPointsProvider = functionProcessor.parse(target, context);
        DataPoints dataPoints = dataPointsProvider.getDataPoints();
        DataPointsParams dataPointsParams = dataPoints.getParams();
        log.debug("Query for target '%s' is :%s", target, dataPoints.getQuery());

        JsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.beginArray();
        System.out.println(dataPoints.getQuery());
        jdbcTemplate.query(
            dataPoints.getQuery(),
            rs -> {
                try {
                    jsonWriter.beginObject();
                    jsonWriter.name("target").value(rs.getString(1));
                    jsonWriter.name("datapoints").beginArray();
                    double[] values = getDoubleArray(rs.getString("values"));
                    for (int i = 0; i < values.length; i++) {
                        jsonWriter.beginArray();
                        double value = values[i];
                        if (Double.isNaN(value)) {
                            jsonWriter.nullValue();
                        } else {
                            jsonWriter.value(value);
                        }
                        int tsSeconds = dataPointsParams.getStartTimeSeconds() + i * dataPointsParams.getStepSeconds();
                        jsonWriter.value(tsSeconds);
                        jsonWriter.endArray();
                    }
                    jsonWriter.endArray();
                    jsonWriter.endObject();
                } catch (IOException e) {
                    throw new RuntimeIOException(e);
                }
            }
        );
        jsonWriter.endArray();
        stopwatch.stop();
        log.info("Executed in {}: {}", stopwatch.toString(), target);

    }

    //TODO optimize
    private double[] getDoubleArray(String string) {
        List<String> strings = getStringList(string);
        double[] array = new double[strings.size()];
        for (int i = 0; i < strings.size(); i++) {
            String value = strings.get(i);
            if (value.equals("inf") || value.equals("nan") || value.equals("NULL")) {
                array[i] = Double.NaN;
            } else {
                array[i] = Double.parseDouble(value);
            }
        }
        return array;
    }

    private List<String> getStringList(String string) {
        if (string.equals("[]")) {
            return Collections.emptyList();
        }
        string = string.substring(1, string.length() - 1);
        String[] splits = string.split(",");
        List<String> strings = new ArrayList<>(splits.length);
        for (String split : splits) {
            strings.add(split);
        }
        return strings;
    }


}
