package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.AutoHideService;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.search.MetricSearch;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a> on 12.01.17
 */
@Configuration
@PropertySource(value = "classpath:graphouse-default.properties")
@PropertySource(
    value = {
        "file:${app.home}/conf/graphouse.properties",
        "file:${app.home}/conf/local-application.properties",
        "file:${app.home}/conf/${environment}/${environment}.properties",
        "file:${app.home}/conf/secret.properties",
        "classpath:local-application.properties"
    },
    ignoreResourceNotFound = true
)
@Import({DbConfig.class, MetricsConfig.class, ServerConfig.class,
    StatisticsConfig.class, StatisticsCountersConfig.class})
public class GraphouseConfig {

    @Autowired
    private MetricSearch metricSearch;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplateAutohide;

    @Bean
    @Primary
    public Monitoring monitoring() {
        return new Monitoring();
    }

    @Bean
    public Monitoring ping() {
        return new Monitoring();
    }

    @Bean(initMethod = "startService")
    public AutoHideService autoHideService() {
        return new AutoHideService(clickHouseJdbcTemplateAutohide, metricSearch);
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
