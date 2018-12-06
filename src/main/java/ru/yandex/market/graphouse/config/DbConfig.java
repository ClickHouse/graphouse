package ru.yandex.market.graphouse.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickhouseJdbcUrlParser;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
@Configuration
public class DbConfig {

    @Bean
    public ClickHouseProperties clickHouseProperties(
        @Value("${graphouse.clickhouse.user}") String user,
        @Value("${graphouse.clickhouse.password}") String password,
        @Value("${graphouse.clickhouse.socket-timeout-seconds}") int socketTimeoutSeconds,
        @Value("${graphouse.clickhouse.connection-timeout-millis}") int socketTimeoutMillis,
        @Value("${graphouse.clickhouse.compress}") boolean compress,
        @Value("${graphouse.clickhouse.max-query-size.bytes}") long maxQuerySizeBytes
    ) {
        final ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setSocketTimeout((int) TimeUnit.SECONDS.toMillis(socketTimeoutSeconds));
        clickHouseProperties.setConnectionTimeout(socketTimeoutMillis);
        clickHouseProperties.setUser(user);
        clickHouseProperties.setPassword(password);
        clickHouseProperties.setCompress(compress);
        clickHouseProperties.setDecompress(compress);
        clickHouseProperties.setUseServerTimeZone(true);
        clickHouseProperties.setUseServerTimeZoneForDates(true);
        clickHouseProperties.setMaxQuerySize(maxQuerySizeBytes);
        return clickHouseProperties;
    }

    @Bean
    public DataSource clickHouseDataSource(@Value("${graphouse.clickhouse.hosts}") String hostsString,
                                           @Value("${graphouse.clickhouse.port}") int port,
                                           @Value("${graphouse.clickhouse.db}") String db,
                                           @Value("${graphouse.clickhouse.host-ping-rate-seconds}") int pingRateSeconds,
                                           ClickHouseProperties clickHouseProperties
    ) {
        List<String> hosts = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(hostsString);
        Preconditions.checkArgument(!hosts.isEmpty(), "ClickHouse host(s) not provided.");
        if (hosts.size() == 1) {
            String url = ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" + hosts.get(0) + ":" + port + "/" + db;
            return new ClickHouseDataSource(url, clickHouseProperties);
        }
        String url = ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" +
            hosts.stream().map(host -> host + ":" + port).collect(Collectors.joining(",")) +
            "/" + db;
        BalancedClickhouseDataSource balancedClickhouseDataSource = new BalancedClickhouseDataSource(
            url, clickHouseProperties
        );
        if (pingRateSeconds > 0) {
            balancedClickhouseDataSource.scheduleActualization(pingRateSeconds, TimeUnit.SECONDS);
        }
        return balancedClickhouseDataSource;
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplate(
        DataSource clickHouseDataSource,
        @Value("${graphouse.clickhouse.query-timeout-seconds}") int queryTimeoutSeconds
    ) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(clickHouseDataSource);
        jdbcTemplate.setQueryTimeout(queryTimeoutSeconds);
        return jdbcTemplate;
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplateAutohide(
        DataSource clickHouseDataSource,
        @Value("${graphouse.autohide.clickhouse.query-timeout-seconds}") int autoHideQueryTimeoutSeconds) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(clickHouseDataSource);
        jdbcTemplate.setQueryTimeout(autoHideQueryTimeoutSeconds);
        return jdbcTemplate;
    }

    @Bean
    public NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate(DataSource clickHouseDataSource) {
        return new NamedParameterJdbcTemplate(clickHouseDataSource);
    }
}
