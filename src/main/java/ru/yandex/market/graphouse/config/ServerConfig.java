package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.market.graphouse.GraphouseWebServer;
import ru.yandex.market.graphouse.MonitoringServlet;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.data.MetricDataService;
import ru.yandex.market.graphouse.data.MetricDataServiceServlet;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricSearchServlet;
import ru.yandex.market.graphouse.server.MetricFactory;
import ru.yandex.market.graphouse.server.MetricServer;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a> on 10.01.17
 */

@Configuration
public class ServerConfig {

    @Autowired
    private MetricSearch metricSearch;

    @Autowired
    private MetricCacher metricCacher;

    @Autowired
    private MetricDataService metricDataService;

    @Autowired
    private MetricFactory metricFactory;

    @Autowired
    private Monitoring monitoring;

    @Autowired
    @Qualifier("ping")
    private Monitoring ping;

    @Bean(initMethod = "startServer")
    public GraphouseWebServer server(
        @Value("${graphouse.metric-data.max-metrics-per-query}") int maxMetricsPerQuery,
        @Value("${graphouse.http.response-buffer-size-bytes}") int responseBufferSizeBytes) {

        MetricSearchServlet metricSearchServlet = new MetricSearchServlet(metricSearch);

        MonitoringServlet monitoringServlet = new MonitoringServlet(monitoring, ping);

        MetricDataServiceServlet metricDataServiceServlet = new MetricDataServiceServlet(
            metricDataService, maxMetricsPerQuery, responseBufferSizeBytes
        );

        return new GraphouseWebServer(metricSearchServlet, monitoringServlet, metricDataServiceServlet);
    }

    @Bean
    public MetricServer metricServer() {
        return new MetricServer(metricCacher, metricFactory);
    }
}
