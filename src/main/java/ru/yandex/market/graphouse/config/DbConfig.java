package ru.yandex.market.graphouse.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickhouseJdbcUrlParser;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.market.graphouse.monitoring.BalancedClickhouseDataSourceMonitoring;
import ru.yandex.market.graphouse.monitoring.Monitoring;

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
        @Value("${graphouse.clickhouse.ssl}") boolean ssl,
        @Value("${graphouse.clickhouse.ssl-root-certificate:}") String sslRootCertificate,
        @Value("${graphouse.clickhouse.max-query-size.bytes}") long maxQuerySizeBytes
    ) {
        final ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setSocketTimeout((int) TimeUnit.SECONDS.toMillis(socketTimeoutSeconds));
        clickHouseProperties.setConnectionTimeout(socketTimeoutMillis);
        clickHouseProperties.setUser(user);
        clickHouseProperties.setPassword(password);
        clickHouseProperties.setCompress(compress);
        clickHouseProperties.setDecompress(compress);
        clickHouseProperties.setSsl(ssl);
        if (!sslRootCertificate.isEmpty()) {
            clickHouseProperties.setSslRootCertificate(sslRootCertificate);
        }

        clickHouseProperties.setUseServerTimeZone(true);
        clickHouseProperties.setUseServerTimeZoneForDates(true);
        clickHouseProperties.setMaxQuerySize(maxQuerySizeBytes);
        return clickHouseProperties;
    }

    @Bean
    public DataSource clickHouseDataSource(
        @Value("${graphouse.clickhouse.hosts}") String hostsString,
        @Value("${graphouse.clickhouse.port}") int port,
        @Value("${graphouse.clickhouse.db}") String db,
        @Value("${graphouse.clickhouse.host-ping-rate-seconds}") int pingRateSeconds,
        ClickHouseProperties clickHouseProperties,
        Monitoring monitoring,
        @Qualifier("ping") Monitoring ping
    ) {
        List<String> hosts = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(hostsString);
        Preconditions.checkArgument(!hosts.isEmpty(), "ClickHouse host(s) not provided.");
        if (hosts.size() == 1) {
            String url = ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" + hosts.get(0) + ":" + port + "/" + db;
            return new ClickHouseDataSource(url, clickHouseProperties);
        }

        return createBalancedClickhouseDataSource(
            hostsString, port, db, pingRateSeconds, clickHouseProperties, monitoring, ping
        );
    }

    @Bean
    public DataSource clickHouseSearchDataSource(
        @Value("${graphouse.clickhouse.hosts}") String hostsString,
        @Value("${graphouse.clickhouse.port}") int port,
        @Value("${graphouse.clickhouse.db}") String db,
        @Value("${graphouse.tree.clickhouse.pool.max-life-time-seconds}") long maxLifeTimeSeconds,
        @Value("${graphouse.tree.clickhouse.pool.max-pool-size}") int maxPoolSize,
        @Value("${graphouse.tree.clickhouse.pool.minimum-idle}") int minimumIdle,
        @Value("${graphouse.tree.clickhouse.pool.validation-timeout-seconds}") int validationTimeoutSeconds,
        @Value("${graphouse.clickhouse.host-ping-rate-seconds}") int pingRateSeconds,
        ClickHouseProperties clickHouseProperties,
        Monitoring monitoring,
        @Qualifier("ping") Monitoring ping
    ) {
        return tryToCreateClickHouseDataSourcePool(
            hostsString,
            port,
            db,
            maxLifeTimeSeconds,
            maxPoolSize,
            minimumIdle,
            validationTimeoutSeconds,
            pingRateSeconds,
            clickHouseProperties,
            monitoring,
            ping
        );
    }

    @Bean
    public DataSource clickHouseOnRecordDataSource(
        @Value("${graphouse.clickhouse.hosts}") String hostsString,
        @Value("${graphouse.clickhouse.port}") int port,
        @Value("${graphouse.clickhouse.db}") String db,
        @Value("${graphouse.on-record-tree.clickhouse.pool.max-life-time-seconds}") long maxLifeTimeSeconds,
        @Value("${graphouse.on-record-tree.clickhouse.pool.max-pool-size}") int maxPoolSize,
        @Value("${graphouse.on-record-tree.clickhouse.pool.minimum-idle}") int minimumIdle,
        @Value("${graphouse.on-record-tree.clickhouse.pool.validation-timeout-seconds}") int validationTimeoutSeconds,
        @Value("${graphouse.clickhouse.host-ping-rate-seconds}") int pingRateSeconds,
        ClickHouseProperties clickHouseProperties,
        Monitoring monitoring,
        @Qualifier("ping") Monitoring ping
    ) {
        return tryToCreateClickHouseDataSourcePool(
            hostsString,
            port,
            db,
            maxLifeTimeSeconds,
            maxPoolSize,
            minimumIdle,
            validationTimeoutSeconds,
            pingRateSeconds,
            clickHouseProperties,
            monitoring,
            ping
        );
    }

    private DataSource tryToCreateClickHouseDataSourcePool(
        String hostsString,
        int port,
        String db,
        long maxLifeTimeSeconds,
        int maxPoolSize,
        int minimumIdle,
        int validationTimeoutSeconds,
        int pingRateSeconds,
        ClickHouseProperties clickHouseProperties,
        Monitoring monitoring,
        Monitoring ping
    ) {
        List<String> hosts = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(hostsString);
        Preconditions.checkArgument(!hosts.isEmpty(), "ClickHouse host(s) not provided.");
        if (hosts.size() == 1) {
            String url = ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" + hosts.get(0) + ":" + port + "/" + db;
            ClickHouseDataSource ds = new ClickHouseDataSource(url, clickHouseProperties);
            return createClickHouseDataSourcePool(
                ds,
                clickHouseProperties.getConnectionTimeout(),
                maxLifeTimeSeconds,
                maxPoolSize,
                minimumIdle,
                validationTimeoutSeconds
            );
        }

        return createBalancedClickhouseDataSource(
            hostsString, port, db, pingRateSeconds, clickHouseProperties, monitoring, ping
        );
    }

    private DataSource createClickHouseDataSourcePool(
        ClickHouseDataSource ds,
        long connectionTimeoutMs,
        long maxLifeTimeSeconds,
        int maxPoolSize,
        int minimumIdle,
        int validationTimeoutSeconds
    ) {
        HikariDataSource hds = new HikariDataSource();
        hds.setDataSource(ds);
        hds.setConnectionTimeout(connectionTimeoutMs);
        hds.setMaxLifetime(TimeUnit.SECONDS.toMillis(maxLifeTimeSeconds)); // does not affect running queries
        hds.setMaximumPoolSize(maxPoolSize);
        hds.setMinimumIdle(minimumIdle);
        hds.setValidationTimeout(TimeUnit.SECONDS.toMillis(validationTimeoutSeconds));
        hds.setAutoCommit(true);

        return hds;
    }

    private DataSource createBalancedClickhouseDataSource(
        String hostsString,
        int port,
        String db,
        int pingRateSeconds,
        ClickHouseProperties clickHouseProperties,
        Monitoring monitoring,
        Monitoring ping
    ) {
        List<String> hosts = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(hostsString);
        String url = ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" +
            hosts.stream().map(host -> host + ":" + port).collect(Collectors.joining(",")) +
            "/" + db;
        BalancedClickhouseDataSource balancedClickhouseDataSource = new BalancedClickhouseDataSource(
            url, clickHouseProperties
        );

        if (pingRateSeconds > 0) {
            int availableServers = balancedClickhouseDataSource.actualize();
            if (availableServers == 0) {
                throw new RuntimeException("Failed to start. All clickhouse servers no available: " + hostsString);
            }

            BalancedClickhouseDataSourceMonitoring dataSourceMonitoring = new BalancedClickhouseDataSourceMonitoring(
                balancedClickhouseDataSource, monitoring, ping, pingRateSeconds
            );
            dataSourceMonitoring.startAsync();
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
        @Value("${graphouse.autohide.clickhouse.query-timeout-seconds}") int autoHideQueryTimeoutSeconds
    ) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(clickHouseDataSource);
        jdbcTemplate.setQueryTimeout(autoHideQueryTimeoutSeconds);
        return jdbcTemplate;
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplateSearch(
        DataSource clickHouseSearchDataSource,
        @Value("${graphouse.clickhouse.query-timeout-seconds}") int queryTimeoutSeconds
    ) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(clickHouseSearchDataSource);
        jdbcTemplate.setQueryTimeout(queryTimeoutSeconds);
        return jdbcTemplate;
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplateOnRecord(
        DataSource clickHouseOnRecordDataSource,
        @Value("${graphouse.clickhouse.query-timeout-seconds}") int queryTimeoutSeconds
    ) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(clickHouseOnRecordDataSource);
        jdbcTemplate.setQueryTimeout(queryTimeoutSeconds);
        return jdbcTemplate;
    }

    @Bean
    public NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate(DataSource clickHouseDataSource) {
        return new NamedParameterJdbcTemplate(clickHouseDataSource);
    }
}
