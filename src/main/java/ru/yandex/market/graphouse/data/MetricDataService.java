package ru.yandex.market.graphouse.data;

import com.google.gson.GsonBuilder;
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


    private String buildQuery(MetricDataParameters parameters) {

        final StringBuilder sqlBuilder = new StringBuilder("SELECT ");

        if (parameters.isMultiMetrics()) {
            sqlBuilder.append("metric, ");
        }

        sqlBuilder.append("kvantT, argMax(value, updated) as Value FROM ").append(graphiteTable).append("\n");

        final String metricNames = parameters.getMetrics().stream().collect(Collectors.joining("', '", "'", "'"));

        if (parameters.isMultiMetrics()) {
            sqlBuilder.append("WHERE metric IN ( ").append(metricNames).append(" ) \n");
        } else {
            sqlBuilder.append("WHERE metric = ").append(metricNames).append("\n");
        }

        sqlBuilder
            .append("AND kvantT >= :startTime AND kvantT <= :endTime \n")
            .append("AND date >= toDate(toDateTime( :startTime )) AND date <= toDate(toDateTime( :endTime )) \n")
            .append("GROUP BY metric, intDiv(toUInt32(Time), :step ) * :step as kvantT \n");


        if (parameters.isMultiMetrics()) {
            sqlBuilder.append("ORDER BY metric, kvantT \n");
        } else {
            sqlBuilder.append("ORDER BY kvantT \n");
        }

        return sqlBuilder.toString();
    }

    public void writeData(MetricDataParameters parameters, Writer resp) throws IOException {
        final long startTime = System.nanoTime();

        final String query = buildQuery(parameters);
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
                        dataResult.appendData(rs.getLong(1), rs.getFloat(2));
                    }
                } catch (IOException e){
                    log.warn("Can't write values to json", e);
                }
            }
        );

        dataResult.flush();

        log.debug(String.format("graphouse_time:[%s] full = %s", parameters.getReqKey(), System.nanoTime() - startTime));
    }

    public void setGraphiteTable(String graphiteTable) {
        this.graphiteTable = graphiteTable;
    }

    @Required
    public void setClickHouseNamedJdbcTemplate(NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate) {
        this.clickHouseNamedJdbcTemplate = clickHouseNamedJdbcTemplate;
    }
}
