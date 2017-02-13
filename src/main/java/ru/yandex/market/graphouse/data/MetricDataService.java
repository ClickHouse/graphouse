package ru.yandex.market.graphouse.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
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

    @Value("${graphite.metric.data.table}")
    private String graphiteTable;

    private final NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate;

    public MetricDataService(NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate) {
        this.clickHouseNamedJdbcTemplate = clickHouseNamedJdbcTemplate;
    }

    private String buildQuery(MetricDataParameters parameters) {

        final StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append("SELECT metric, kvantT, argMax(value, updated) as value FROM ").append(graphiteTable).append("\n");

        final String metricNames = parameters.getMetrics().stream().collect(Collectors.joining("', '", "'", "'"));
        sqlBuilder.append("WHERE metric IN ( ").append(metricNames).append(" ) \n");

        sqlBuilder
            .append("AND kvantT >= :startTime AND kvantT <= :endTime \n")
            .append("AND date >= toDate(toDateTime( :startTime )) AND date <= toDate(toDateTime( :endTime )) \n")
            .append("GROUP BY metric, intDiv(toUInt32(timestamp), :step ) * :step as kvantT \n")
            .append("ORDER BY metric, kvantT \n");

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

        try {
            clickHouseNamedJdbcTemplate.query(
                query,
                queryParams,
                rs -> {
                    try {
                        dataResult.appendData(rs.getString(1), rs.getLong(2), rs.getFloat(3));
                    } catch (IOException e){
                        log.error("Can't write values to json");
                        throw new RuntimeException(e);
                    }
                }
            );
        } catch (RuntimeException e) {
            log.error("Data request failed", e);
            throw e;
        }

        dataResult.flush();

        log.debug(String.format("graphouse_time:[%s] full = %s", parameters.getReqKey(), System.nanoTime() - startTime));
    }
}
