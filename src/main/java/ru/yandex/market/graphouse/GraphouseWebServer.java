package ru.yandex.market.graphouse;

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
import org.springframework.beans.factory.InitializingBean;
import ru.yandex.market.graphouse.data.MetricDataServiceServlet;
import ru.yandex.market.graphouse.search.MetricSearchServlet;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/04/15
 */
public class GraphouseWebServer implements InitializingBean {

    private static final Logger log = LogManager.getLogger();

    private int metricSearchPort = 7000;
    private int threadCount = 20;

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

    @Override
    public void afterPropertiesSet() throws Exception {
        startServer();
    }

    private void startServer() throws Exception {
        Server server = new Server(new QueuedThreadPool(threadCount));
        ServerConnector serverConnector = new ServerConnector(server);
        serverConnector.setPort(metricSearchPort);
        server.setConnectors(new Connector[]{serverConnector});
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        ServletHolder metricSearchServletHolder = new ServletHolder(metricSearchServlet);
        context.addServlet(metricSearchServletHolder, "/search/*");
        context.addServlet(metricSearchServletHolder, "/ban/*");
        context.addServlet(metricSearchServletHolder, "/multiBan/*");
        context.addServlet(metricSearchServletHolder, "/approve/*");
        context.addServlet(metricSearchServletHolder, "/multiApprove/*");
        context.addServlet(metricSearchServletHolder, "/hide/*");
        context.addServlet(metricSearchServletHolder, "/multiHide/*");

        ServletHolder monitoringServletHolder = new ServletHolder(monitoringServlet);
        context.addServlet(monitoringServletHolder, "/ping");
        context.addServlet(monitoringServletHolder, "/monitoring");

        ServletHolder metricDataServletHolder = new ServletHolder(metricDataServiceServlet);
        context.addServlet(metricDataServletHolder, "/metricData");

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler()});
        server.setHandler(handlers);
        server.start();
    }

    public void setMetricSearchPort(int metricSearchPort) {
        this.metricSearchPort = metricSearchPort;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

}
