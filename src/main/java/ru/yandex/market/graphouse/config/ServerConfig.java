package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
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

    @Value("${graphouse.allow-cold-run}")
    private boolean allowColdRun = false;

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

    @Bean(initMethod = "startServer")
    public GraphouseWebServer graphouseWebServer() {

        final MetricSearchServlet metricSearchServlet = new MetricSearchServlet(metricSearch, allowColdRun);

        final MonitoringServlet monitoringServlet = new MonitoringServlet(monitoring, metricSearch, allowColdRun);

        final MetricDataServiceServlet metricDataServiceServlet = new MetricDataServiceServlet(metricDataService);

        return new GraphouseWebServer(metricSearchServlet, monitoringServlet, metricDataServiceServlet);
    }

    @Bean
    public MetricServer metricServer() {
        return new MetricServer(metricCacher, metricFactory);
    }
}
