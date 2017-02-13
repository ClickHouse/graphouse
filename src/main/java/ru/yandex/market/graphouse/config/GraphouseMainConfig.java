package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.AutoHideService;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.search.MetricSearch;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a> on 12.01.17
 */
@Configuration
@ImportResource("classpath:property-configurer.xml")
@Import({DbConfig.class, MetricsConfig.class, ServerConfig.class})
public class GraphouseMainConfig {

    @Autowired
    private MetricSearch metricSearch;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Bean
    public Monitoring monitoring() {
        return new Monitoring();
    }

    @Bean(initMethod = "startService")
    public AutoHideService autoHideService() {
        return new AutoHideService(clickHouseJdbcTemplate, metricSearch);
    }

}
