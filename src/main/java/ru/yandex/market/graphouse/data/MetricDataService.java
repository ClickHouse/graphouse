package ru.yandex.market.graphouse.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.market.graphouse.search.MetricDataRetention;
import ru.yandex.market.graphouse.search.MetricDescription;
import ru.yandex.market.graphouse.search.MetricSearch;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 31.10.16
 */

public class MetricDataService {

    private static final Logger log = LogManager.getLogger();

    private NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate;

    private String graphiteTable;

    private MetricSearch metricSearch;

    private int defaultStepSize = 60;
    private String defaultFunction = "avg";

    private final MetricDataRetention DEFAULT_DATA_RETENTION;

    public MetricDataService() {
        final int month = (int) TimeUnit.DAYS.toSeconds(30);
        DEFAULT_DATA_RETENTION = new MetricDataRetention.MetricDataRetentionBuilder("*.", defaultFunction)
            .addRetention(0, defaultStepSize)
            .addRetention(month, 5 * defaultStepSize)
            .addRetention(12 * month, 30 * defaultStepSize)
            .build();
    }

    private String buildQuery(Map<String, GroupParameters> parameters) {
        final Map<GroupParameters, List<String>> groups = new HashMap<>();

        parameters.entrySet().forEach(o -> groups.computeIfAbsent(o.getValue(), k -> new ArrayList<>()).add(o.getKey()));

        final StringBuilder sqlBuilder = new StringBuilder();
        final boolean isMultiGroups = (parameters.size() > 1);

        if (isMultiGroups) {
            sqlBuilder.append("SELECT metric, quantT, value FROM (");
        }

        final String sqlTemplate = "" +
            " SELECT metric, quantT, %s(value) as value" +
            " FROM (" +
            "   SELECT metric, timestamp, argMax(value, updated) as value " +
            "   FROM " + graphiteTable +
            "   WHERE metric IN ( %s ) " +
            "   AND date >= toDate(toDateTime(%d)) AND date <= toDate(toDateTime(%d)) " +
            "   GROUP BY metric, timestamp) " +
            " WHERE quantT >= %d AND quantT <= %d " +
            " GROUP BY metric, intDiv(toUInt32(timestamp), %d) * %d as quantT";

        int groupCounter = 0;
        for (Map.Entry<GroupParameters, List<String>> parameter : groups.entrySet()) {

            final GroupParameters groupParameters = parameter.getKey();
            final String metricNames = parameter.getValue().stream().collect(Collectors.joining("', '", "'", "'"));

            sqlBuilder.append(
                String.format(sqlTemplate,
                    groupParameters.function,
                    metricNames,
                    groupParameters.startTimeSeconds,
                    groupParameters.endTimeSeconds,
                    groupParameters.startTimeSeconds,
                    groupParameters.endTimeSeconds,
                    groupParameters.stepSizeInSeconds,
                    groupParameters.stepSizeInSeconds
                ));

            if (isMultiGroups) {
                final boolean isLast = (groupCounter == groups.keySet().size() - 1);
                if (!isLast) {
                    sqlBuilder.append("\n UNION ALL \n");
                }
            }

            groupCounter++;
        }

        if (isMultiGroups) {
            sqlBuilder.append(")");
        }

        sqlBuilder.append(" ORDER BY metric, quantT");

        return sqlBuilder.toString();
    }

    void writeData(Writer resp, List<String> metrics, int startTimeSeconds, int endTimeSeconds, String reqKey) throws IOException {
        final long startTime = System.nanoTime();

        final Map<String, GroupParameters> parametersMap = new HashMap<>();
        final int currentSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        for (String metricName : metrics) {
            final int stepSize;
            final String function;

            final MetricDescription metricDescription = metricSearch.getMetricDescription(metricName);
            final MetricDataRetention retention = (metricDescription == null) ? DEFAULT_DATA_RETENTION : metricDescription.getDataRetention();

            stepSize = retention.getStepSize(currentSeconds - startTimeSeconds);
            function = retention.getFunction();

            final GroupParameters groupParameters = new GroupParameters(startTimeSeconds, endTimeSeconds, stepSize, function);
            parametersMap.put(metricName, groupParameters);
        }

        final String query = buildQuery(parametersMap);
        log.debug("Request = \n" + query);

        final MetricDataResult dataResult = new MetricDataResult(resp, parametersMap);

        clickHouseNamedJdbcTemplate.query(
            query,
            rs -> {
                try {
                    dataResult.appendData(rs.getString(1), rs.getLong(2), rs.getFloat(3));
                } catch (IOException e) {
                    log.warn("Can't write values to json", e);
                }
            }
        );

        dataResult.flush();

        log.debug(String.format("graphouse_time:[%s] full = %s", reqKey, System.nanoTime() - startTime));
    }

    public void setGraphiteTable(String graphiteTable) {
        this.graphiteTable = graphiteTable;
    }

    @Required
    public void setClickHouseNamedJdbcTemplate(NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate) {
        this.clickHouseNamedJdbcTemplate = clickHouseNamedJdbcTemplate;
    }

    @Required
    public void setMetricSearch(MetricSearch metricSearch) {
        this.metricSearch = metricSearch;
    }

    public void setDefaultStepSize(int defaultStepSize) {
        this.defaultStepSize = defaultStepSize;
    }

    public void setDefaultFunction(String defaultFunction) {
        this.defaultFunction = defaultFunction;
    }

    class GroupParameters {
        private final int startTimeSeconds;
        private final int endTimeSeconds;
        private final int stepSizeInSeconds;
        private final String function;
        private final int pointsCount;

        private GroupParameters(int startTimeSeconds, int endTimeSeconds, int metricStepInSeconds, String function) {
            this.stepSizeInSeconds = metricStepInSeconds;
            this.startTimeSeconds = startTimeSeconds - startTimeSeconds % metricStepInSeconds;
            this.endTimeSeconds = endTimeSeconds - endTimeSeconds % metricStepInSeconds;
            this.pointsCount = (this.endTimeSeconds - this.startTimeSeconds) / metricStepInSeconds + 1;
            this.function = function;
        }

        int getStartTimeSeconds() {
            return startTimeSeconds;
        }

        int getEndTimeSeconds() {
            return endTimeSeconds;
        }

        int getStepSizeInSeconds() {
            return stepSizeInSeconds;
        }

        String getFunction() {
            return function;
        }

        int getPointsCount() {
            return pointsCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GroupParameters that = (GroupParameters) o;

            if (startTimeSeconds != that.startTimeSeconds) {
                return false;
            }
            if (endTimeSeconds != that.endTimeSeconds) {
                return false;
            }
            if (stepSizeInSeconds != that.stepSizeInSeconds) {
                return false;
            }
            return function.equals(that.function);
        }

        @Override
        public int hashCode() {
            int result = startTimeSeconds;
            result = 31 * result + endTimeSeconds;
            result = 31 * result + stepSizeInSeconds;
            result = 31 * result + function.hashCode();
            return result;
        }
    }
}
