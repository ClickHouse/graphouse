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
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 08/04/15
 */
public class GraphouseWebServer implements InitializingBean {

    private static final Logger log = LogManager.getLogger();

    private int metricSearchPort = 7000;
    private int threadCount = 20;

    private MetricSearchServlet metricSearchServlet;

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
        context.addServlet(metricSearchServletHolder, "/ban/*");
        context.addServlet(metricSearchServletHolder, "/search/*");
        //TODO add ping!!
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler()});
        server.setHandler(handlers);
        server.start();
        server.join();
    }

    @Required
    public void setMetricSearchServlet(MetricSearchServlet metricSearchServlet) {
        this.metricSearchServlet = metricSearchServlet;
    }

    public void setMetricSearchPort(int metricSearchPort) {
        this.metricSearchPort = metricSearchPort;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
