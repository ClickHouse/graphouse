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
import ru.yandex.market.graphouse.save.OnRecordCacheUpdater;
import ru.yandex.market.graphouse.save.OnRecordMetricProvider;
import ru.yandex.market.graphouse.save.OnRecordMetricCacheServlet;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricSearchServlet;
import ru.yandex.market.graphouse.server.MetricServer;
import ru.yandex.market.graphouse.server.OnRecordCacheBasedMetricFactory;
import ru.yandex.market.graphouse.server.SearchCacheBasedMetricFactory;
import ru.yandex.market.graphouse.statistics.StatisticsService;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a> on 10.01.17
 */

@Configuration
public class ServerConfig {

    @Autowired
    private MetricSearch metricSearch;

    @Autowired
    private OnRecordCacheUpdater onRecordCacheUpdater;

    @Autowired
    private OnRecordMetricProvider onRecordMetricProvider;

    @Autowired
    private MetricCacher metricCacher;

    @Autowired
    private MetricDataService metricDataService;

    @Autowired
    private SearchCacheBasedMetricFactory searchMetricFactory;

    @Autowired
    private OnRecordCacheBasedMetricFactory onRecordMetricFactory;

    @Autowired
    private Monitoring monitoring;

    @Autowired
    private StatisticsService statisticsService;

    @Autowired
    @Qualifier("ping")
    private Monitoring ping;

    @Bean(initMethod = "startServer")
    public GraphouseWebServer server(
        @Value("${graphouse.metric-data.max-metrics-per-query}") int maxMetricsPerQuery,
        @Value("${graphouse.http.response-buffer-size-bytes}") int responseBufferSizeBytes) {

        final MetricSearchServlet metricSearchServlet = new MetricSearchServlet(
            metricSearch, statisticsService
        );

        MonitoringServlet monitoringServlet = new MonitoringServlet(monitoring, ping);

        MetricDataServiceServlet metricDataServiceServlet = new MetricDataServiceServlet(
            metricDataService, maxMetricsPerQuery, responseBufferSizeBytes
        );

        OnRecordMetricCacheServlet onRecordMetricCacheServlet = new OnRecordMetricCacheServlet(
            onRecordCacheUpdater, onRecordMetricProvider, statisticsService
        );

        return new GraphouseWebServer(
            metricSearchServlet,
            monitoringServlet,
            metricDataServiceServlet,
            onRecordMetricCacheServlet
        );
    }

    @Bean
    public MetricServer metricServer(
        @Value("${graphouse.on-record-metric-cache.enable}") boolean onRecordCacheEnable
    ) {
        return new MetricServer(metricCacher, onRecordCacheEnable ? onRecordMetricFactory : searchMetricFactory);
    }
}
