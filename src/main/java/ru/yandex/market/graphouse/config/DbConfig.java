package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.util.concurrent.TimeUnit;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
@Configuration
public class DbConfig {

    @Value("${graphouse.clickhouse.host}")
    private String clickhouseHost;

    @Value("${graphouse.clickhouse.port}")
    private String clickhousePort;

    @Value("${graphouse.clickhouse.db}")
    private String clickhouseDatabase;

    @Value("${graphouse.clickhouse.user}")
    private String clickhouseUsername;

    @Value("${graphouse.clickhouse.password}")
    private String clickhousePassword;

    @Value("${graphouse.clickhouse.socket-timeout-seconds}")
    private int clickhouseSocketTimeoutSeconds;

    @Value("${graphouse.clickhouse.query-timeout-seconds}")
    private int clickhouseQueryTimeoutSeconds;

    @Value("${graphouse.autohide.clickhouse.query-timeout-seconds}")
    private int clickhouseAutohideQueryTimeoutSeconds;

    @Value("${graphouse.clickhouse.compress}")
    private boolean compress;

    @Bean
    public ClickHouseDataSource clickHouseDataSource() {
        final String url = "jdbc:clickhouse://" + clickhouseHost + ":" + clickhousePort + "/" + clickhouseDatabase;

        final ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setSocketTimeout((int) TimeUnit.SECONDS.toMillis(clickhouseSocketTimeoutSeconds));
        clickHouseProperties.setUser(clickhouseUsername);
        clickHouseProperties.setPassword(clickhousePassword);
        clickHouseProperties.setDecompress(compress);
        clickHouseProperties.setDecompress(compress);
        return new ClickHouseDataSource(url, clickHouseProperties);
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplate(ClickHouseDataSource clickHouseDataSource) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(clickHouseDataSource);
        jdbcTemplate.setQueryTimeout(clickhouseQueryTimeoutSeconds);

        return jdbcTemplate;
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplateAutohide(ClickHouseDataSource clickHouseDataSource) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(clickHouseDataSource);
        jdbcTemplate.setQueryTimeout(clickhouseAutohideQueryTimeoutSeconds);

        return jdbcTemplate;
    }

    @Bean
    public NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate(ClickHouseDataSource clickHouseDataSource) {
        return new NamedParameterJdbcTemplate(clickHouseDataSource);
    }
}
