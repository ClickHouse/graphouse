package ru.yandex.market.graphouse.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import ru.yandex.market.graphouse.monitoring.Monitoring;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a> on 12.01.17
 */
@Configuration
@ImportResource("classpath:property-configurer.xml")
@Import({DbConfig.class, MetricsConfig.class, ServerConfig.class, AutohideConfig.class})
public class GraphouseMainConfig {

    @Bean
    public Monitoring monitoring() {
        return new Monitoring();
    }

}
