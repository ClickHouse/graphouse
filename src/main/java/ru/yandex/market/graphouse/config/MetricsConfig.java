package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.data.MetricDataService;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.server.MetricFactory;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
@Configuration
public class MetricsConfig {

    @Autowired
    private Monitoring monitoring;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate;

    @Bean
    public MetricSearch metricSearch(MetricValidator metricValidator) {
        return new MetricSearch(clickHouseJdbcTemplate, monitoring, metricValidator);
    }

    @Bean
    public MetricDataService metricDataService() {
        return new MetricDataService(clickHouseNamedJdbcTemplate);
    }

    @Bean
    public MetricValidator metricValidator() {
        return new MetricValidator();
    }

    @Bean
    public MetricCacher metricCacher() {
        return new MetricCacher(clickHouseJdbcTemplate, monitoring);
    }

    @Bean
    public MetricFactory metricFactory(MetricValidator metricValidator) {
        return new MetricFactory(metricSearch(metricValidator), metricValidator);
    }
}
