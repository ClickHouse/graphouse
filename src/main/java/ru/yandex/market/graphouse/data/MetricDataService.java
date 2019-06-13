package ru.yandex.market.graphouse.data;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.tree.MetricName;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    private final int maxPointsPerMetric;

    public MetricDataService(MetricSearch metricSearch, JdbcTemplate clickHouseJdbcTemplate,
                             String graphiteDataReadTable, int maxPointsPerMetric) {
        this.metricSearch = metricSearch;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.graphiteDataReadTable = graphiteDataReadTable;
        this.maxPointsPerMetric = maxPointsPerMetric;
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

        MetricDataQueryParams queryParams = MetricDataQueryParams.create(
            metrics, startTimeSeconds, endTimeSeconds, maxPointsPerMetric
        );

        Set<String> metricsSet = new HashSet<>();
        String metricString = metrics.stream()
            .map(MetricName::getName)
            .peek(metricsSet::add)
            .collect(Collectors.joining("','", "'", "'"));


        MetricDataRowCallbackHandler handler = new MetricDataRowCallbackHandler(jsonWriter, queryParams, metricsSet);

        clickHouseJdbcTemplate.query(
            "SELECT metric, ts, " + function + "(value) as value FROM (" +
                "   SELECT metric, ts, argMax(value, updated) as value FROM " + graphiteDataReadTable +
                "       WHERE metric IN (" + metricString + ")" +
                "           AND ts >= ? AND ts < ? AND date >= toDate(?) AND date <= toDate(?)" +
                "       GROUP BY metric, timestamp as ts" +
                ") GROUP BY metric, intDiv(toUInt32(ts), ?) * ? as ts ORDER BY metric, ts",
            handler,
            queryParams.getStartTimeSeconds(), queryParams.getEndTimeSeconds(),
            queryParams.getStartTimeSeconds(), queryParams.getEndTimeSeconds(),
            queryParams.getStepSeconds(), queryParams.getStepSeconds()
        );
        handler.finish();
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
