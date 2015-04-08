package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.clickhouse.ClickhouseTemplate;
import ru.yandex.market.graphouse.Metric;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 07/04/15
 */
public class MetricSearch implements InitializingBean, Runnable {

    private static final Logger log = LogManager.getLogger();

    private static final int FLUSH_INTERVAL_MILLIS = 30_000;
    private ClickhouseTemplate clickhouseTemplate;

    private final MetricTree metricTree = new MetricTree();
    private Queue<String> newMetricQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void afterPropertiesSet() throws Exception {

        //Read metrics
        //Read bans
        new Thread(this, "MetricSearch thread").start();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(FLUSH_INTERVAL_MILLIS);
            } catch (InterruptedException ignored) {
            }
            if (!newMetricQueue.isEmpty()){
                log.info("Saving " + newMetricQueue.size() + " metric names");
            }
            //TODO save thread
            //CH update thread
        }

    }

    public void add(String metric) {
        if (metricTree.add(metric)) {
            newMetricQueue.add(metric);
        }
    }

    public void ban(String metric) {
        //TODO save baned
        metricTree.ban(metric);
    }

    public String search(String query) {
        return metricTree.search(query);
    }

    @Required
    public void setClickhouseTemplate(ClickhouseTemplate clickhouseTemplate) {
        this.clickhouseTemplate = clickhouseTemplate;
    }
}
