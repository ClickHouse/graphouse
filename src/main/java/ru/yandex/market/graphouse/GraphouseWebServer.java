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
import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.graphouse.search.MetricSearchServlet;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/04/15
 */
public class GraphouseWebServer implements InitializingBean{

    private static final Logger log = LogManager.getLogger();

    private int metricSearchPort = 7000;
    private int threadCount = 20;

    private MetricSearchServlet metricSearchServlet;
    private MonitoringServlet monitoringServlet;

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
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler()});
        server.setHandler(handlers);
        server.start();
    }

    @Required
    public void setMetricSearchServlet(MetricSearchServlet metricSearchServlet) {
        this.metricSearchServlet = metricSearchServlet;
    }

    @Required
    public void setMonitoringServlet(MonitoringServlet monitoringServlet) {
        this.monitoringServlet = monitoringServlet;
    }

    public void setMetricSearchPort(int metricSearchPort) {
        this.metricSearchPort = metricSearchPort;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
