package ru.yandex.market.graphouse;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.market.graphouse.data.MetricDataServiceServlet;
import ru.yandex.market.graphouse.search.MetricSearchServlet;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/04/15
 */
public class GraphouseWebServer {

    private static final Logger log = LogManager.getLogger();

    @Value("${graphouse.http.port}")
    private int httpPort;

    @Value("${graphouse.http.bind-address}")
    private String httpBindAddress;

    @Value("${graphouse.http.max-form-context-size-bytes}")
    private int maxFormContextSizeBytes;

    @Value("${graphouse.http.threads}")
    private int threadCount;

    private final MetricSearchServlet metricSearchServlet;
    private final MonitoringServlet monitoringServlet;

    private final MetricDataServiceServlet metricDataServiceServlet;

    public GraphouseWebServer(MetricSearchServlet metricSearchServlet,
                              MonitoringServlet monitoringServlet,
                              MetricDataServiceServlet metricDataServiceServlet) {
        this.metricSearchServlet = metricSearchServlet;
        this.monitoringServlet = monitoringServlet;
        this.metricDataServiceServlet = metricDataServiceServlet;
    }

    private void startServer() throws Exception {

        log.info("Starting http server on port " + httpPort);
        Server server = new Server(new QueuedThreadPool(threadCount));
        ServerConnector serverConnector = new ServerConnector(server);
        serverConnector.setPort(httpPort);
        if (!Strings.isNullOrEmpty(httpBindAddress)) {
            serverConnector.setHost(httpBindAddress);
        }
        server.setConnectors(new Connector[]{serverConnector});
        ServletContextHandler context = new ServletContextHandler();
        context.setMaxFormContentSize(maxFormContextSizeBytes);
        context.setContextPath("/");

        ServletHolder metricSearchServletHolder = new ServletHolder(metricSearchServlet);
        context.addServlet(metricSearchServletHolder, "/search/*");
        context.addServlet(metricSearchServletHolder, "/ban/*");
        context.addServlet(metricSearchServletHolder, "/multiBan/*");
        context.addServlet(metricSearchServletHolder, "/approve/*");
        context.addServlet(metricSearchServletHolder, "/multiApprove/*");
        context.addServlet(metricSearchServletHolder, "/hide/*");
        context.addServlet(metricSearchServletHolder, "/multiHide/*");
        context.addServlet(metricSearchServletHolder, "/searchCachedMetrics/*");

        ServletHolder monitoringServletHolder = new ServletHolder(monitoringServlet);
        context.addServlet(monitoringServletHolder, "/ping");
        context.addServlet(monitoringServletHolder, "/monitoring");

        ServletHolder metricDataServletHolder = new ServletHolder(metricDataServiceServlet);
        context.addServlet(metricDataServletHolder, "/metricData");

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler()});
        server.setHandler(handlers);
        server.start();

        log.info("Web server started on port " + httpPort);
    }
}
