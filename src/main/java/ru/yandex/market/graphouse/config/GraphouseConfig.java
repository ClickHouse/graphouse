package ru.yandex.market.graphouse.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import ru.yandex.market.graphouse.monitoring.Monitoring;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a> on 12.01.17
 */
@Configuration
@PropertySource(value = "classpath:graphouse-default.properties")
@PropertySource(
    value = {
        "file:${app.home}/conf/graphouse.properties",
        "file:${app.home}/conf/local-application.properties",
        "classpath:local-application.properties"
    },
    ignoreResourceNotFound = true
)
@Import({DbConfig.class, MetricsConfig.class, ServerConfig.class})
public class GraphouseConfig {

    @Bean
    public Monitoring monitoring() {
        return new Monitoring();
    }


//    @Bean
//    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
//        return new PropertySourcesPlaceholderConfigurer();
//    }

}
