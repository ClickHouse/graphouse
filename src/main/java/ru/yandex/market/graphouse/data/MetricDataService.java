package ru.yandex.market.graphouse.data;

import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.tree.MetricName;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/04/2017
 */
public class MetricDataService {

    private final MetricSearch metricSearch;

    private final JdbcTemplate clickHouseJdbcTemplate;

    private final String graphiteTable;

    public MetricDataService(MetricSearch metricSearch, JdbcTemplate clickHouseJdbcTemplate, String graphiteTable) {
        this.metricSearch = metricSearch;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.graphiteTable = graphiteTable;
    }


    public void getData(List<String> metricStrings, int startTimeSeconds, int endTimeSeconds,
                        PrintWriter printWriter) throws Exception {


        List<MetricName> metrics = getMetrics(metricStrings);

    }

    private List<MetricName> getMetrics(List<String> metricStrings) throws IOException {
        List<MetricName> metrics = new ArrayList<>(metricStrings.size());

        for (String metricString : metricStrings) {
            metricSearch.search(metricString, metric -> {
                if (metric instanceof MetricName) {
                    metrics.add((MetricName) metric);
                }
            });
        }
        return metrics;

    }


}
