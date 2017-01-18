package ru.yandex.market.graphouse.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 31.10.16
 */

public class MetricDataService {

    private static final Logger log = LogManager.getLogger();

    private NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate;

    private String graphiteTable;

    private boolean migrationSchemeEnabled;

    private String buildQuery(MetricDataParameters parameters) {

        final StringBuilder sqlBuilder = new StringBuilder();

        //TODO: remove updated after migration
        sqlBuilder.append("SELECT metric, kvantT, argMax(value, updated) as value, max(updated) as max_updated FROM ").append(graphiteTable).append("\n");

        final String metricNames = parameters.getMetrics().stream().collect(Collectors.joining("', '", "'", "'"));
        sqlBuilder.append("WHERE metric IN ( ").append(metricNames).append(" ) \n");

        sqlBuilder
            .append("AND kvantT >= :startTime AND kvantT <= :endTime \n")
            .append("AND date >= toDate(toDateTime( :startTime )) AND date <= toDate(toDateTime( :endTime )) \n")
            .append("GROUP BY metric, intDiv(toUInt32(timestamp), :step ) * :step as kvantT \n")
            .append("ORDER BY metric, kvantT \n");

        return sqlBuilder.toString();
    }

    @Deprecated
    private String bildOldTableQuery(MetricDataParameters parameters) {
        final StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append("SELECT Path as metric, kvantT, argMax(Value, Timestamp) as value, max(Timestamp) as max_updated FROM graphite_old \n");

        final String metricNames = parameters.getMetrics().stream().collect(Collectors.joining("', '", "'", "'"));
        sqlBuilder.append("WHERE Path IN ( ").append(metricNames).append(" ) \n");

        sqlBuilder
            .append("AND kvantT >= :startTime AND kvantT <= :endTime \n")
            .append("AND Date >= toDate(toDateTime( :startTime )) AND Date <= toDate(toDateTime( :endTime )) \n")
            .append("GROUP BY Path, intDiv(toUInt32(Time), :step ) * :step as kvantT \n")
            .append("ORDER BY metric, kvantT \n");

        return sqlBuilder.toString();
    }

    @Deprecated
    private String appendOldTableUnion(String query, MetricDataParameters parameters) {
        final String oldTableQuery = bildOldTableQuery(parameters);

        return "SELECT metric, kvantT, argMax(value, max_updated) as value FROM (" + query + " UNION ALL " + oldTableQuery + ") GROUP BY metric, kvantT ORDER BY metric, kvantT";
    }


    public void writeData(MetricDataParameters parameters, Writer resp) throws IOException {
        final long startTime = System.nanoTime();

        String query = buildQuery(parameters);
        if (migrationSchemeEnabled) {
            query = appendOldTableUnion(query, parameters);
        }

        log.debug("Request = \n" + query);

        final Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("startTime", parameters.getStartTimeSeconds());
        queryParams.put("endTime", parameters.getEndTimeSeconds());
        queryParams.put("step", parameters.getMetricStep().getStepSizeInSeconds());

        final MetricDataResult dataResult = new MetricDataResult(parameters, resp);

        clickHouseNamedJdbcTemplate.query(
            query,
            queryParams,
            rs -> {
                try {
                    if (parameters.isMultiMetrics()) {
                        dataResult.appendData(rs.getString(1), rs.getLong(2), rs.getFloat(3));
                    } else {
                        dataResult.appendData(rs.getLong(2), rs.getFloat(3));
                    }
                } catch (IOException e){
                    log.warn("Can't write values to json", e);
                }
            }
        );

        dataResult.flush();

        log.debug(String.format("graphouse_time:[%s] full = %s", parameters.getReqKey(), System.nanoTime() - startTime));
    }

    @Required
    public void setGraphiteTable(String graphiteTable) {
        this.graphiteTable = graphiteTable;
    }

    @Required
    public void setClickHouseNamedJdbcTemplate(NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate) {
        this.clickHouseNamedJdbcTemplate = clickHouseNamedJdbcTemplate;
    }

    public void setMigrationSchemeEnabled(boolean migrationSchemeEnabled) {
        this.migrationSchemeEnabled = migrationSchemeEnabled;
    }
}
