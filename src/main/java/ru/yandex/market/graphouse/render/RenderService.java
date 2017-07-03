package ru.yandex.market.graphouse.render;

import com.google.common.base.Stopwatch;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.io.RuntimeIOException;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.tree.MetricDescription;
import ru.yandex.market.graphouse.utils.AppendableList;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 18/02/2017
 */
public class RenderService {

    private static final Logger log = LogManager.getLogger();

    private final MetricSearch metricSearch;
    private final FunctionProcessor functionProcessor;
    private final JdbcTemplate jdbcTemplate;

    public RenderService(MetricSearch metricSearch, FunctionProcessor functionProcessor, JdbcTemplate jdbcTemplate) {
        this.metricSearch = metricSearch;
        this.functionProcessor = functionProcessor;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void doWork(int startTimeSeconds, int endTimeSeconds, String target, int maxDataPoints,
                       PrintWriter writer) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        int timeSeconds = endTimeSeconds - startTimeSeconds;
        int step = 60;
        int dataPoints = timeSeconds / step;
        if (maxDataPoints > 0 && dataPoints < maxDataPoints) {
            int valuesPerPoint = (int) Math.ceil((double) dataPoints / maxDataPoints);
            step *= valuesPerPoint;
            dataPoints = timeSeconds / step;
        }
        startTimeSeconds = (endTimeSeconds + 1) - (step * dataPoints);


        FunctionProcessor.FunctionWrapper wrapper = functionProcessor.parse(target);
        QueryBuilder queryBuilder = doChain(wrapper, startTimeSeconds, endTimeSeconds, step);

        final int finalDataPoints = dataPoints;
        final int finalStartTimeSeconds = startTimeSeconds;
        final int finalStep = step;

        String query = queryBuilder.getQuery();
        log.info(query);

        JsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.beginArray();

        jdbcTemplate.query(
            query,
            rs -> {
                try {
                    jsonWriter.beginObject();
                    jsonWriter.name("target").value(rs.getString(1));
                    jsonWriter.name("datapoints").beginArray();
                    int[] tss = getIntArray(rs.getString("tss"));
                    double[] rawValues = getDoubleArray(rs.getString("values"));
                    double[] values = new double[finalDataPoints];
                    Arrays.fill(values, Double.NaN);
                    for (int i = 0; i < tss.length; i++) {
                        int position = (tss[i] - finalStartTimeSeconds) / finalStep;
                        values[position] = rawValues[i];
                    }

                    for (int i = 0; i < values.length; i++) {
                        int ts = finalStartTimeSeconds + (finalStep * i);
                        jsonWriter.beginArray();
                        double value = values[i];
                        if (Double.isNaN(value)) {
                            jsonWriter.nullValue();
                        } else {
                            jsonWriter.value(value);
                        }

                        jsonWriter.value(ts);
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
        log.info("Executed in " + stopwatch.toString() + ": " + target);


    }

    private int[] getIntArray(String string) {
        List<String> strings = getStringList(string);
        int[] array = new int[strings.size()];
        for (int i = 0; i < strings.size(); i++) {
            array[i] = Integer.parseInt(strings.get(i));
        }
        return array;
    }

    private double[] getDoubleArray(String string) {
        List<String> strings = getStringList(string);
        double[] array = new double[strings.size()];
        for (int i = 0; i < strings.size(); i++) {
            array[i] = Double.parseDouble(strings.get(i));
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


    private QueryBuilder doChain(FunctionProcessor.FunctionWrapper wrapper, int start, int end, int step) throws IOException {
        if (wrapper.getSubFunction() == null) {
            String path = wrapper.getArgs()[0];
            AppendableList appendableList = new AppendableList();
            metricSearch.search(path, appendableList);
//            String table = "remote ('health-house.market.yandex.net', graphite, data_all)";
            String table = "hdd.data";
            return QueryBuilder.create(
                table, path,
                appendableList.getList().stream().map(MetricDescription::getName).collect(Collectors.toList()),
                "avg", start, end, step
            );
        }
        QueryBuilder queryBuilder = doChain(wrapper.getSubFunction(), start, end, step);
        wrapper.getFunction().apply(queryBuilder, wrapper.getArgs());
        return queryBuilder;
    }


}
