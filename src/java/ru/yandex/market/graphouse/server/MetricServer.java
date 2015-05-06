package ru.yandex.market.graphouse.server;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 02/04/15
 */
public class MetricServer implements InitializingBean {

    private static final Logger log = LogManager.getLogger();

    //TODO расфасовать
    private int port = 2024;
    private ServerSocket serverSocket;
    private int socketTimeoutMillis = 5000;
    private int threadCount = 20;
    private MetricCacher metricCacher;
    private MetricSearch metricSearch;
    private ExecutorService executorService;

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("Starting metric server on port: " + port);
        serverSocket = new ServerSocket(port);

        log.info("Starting " + threadCount + " metric server threads");
        executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(new MetricServerWorker());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Shutting down metric server");
                executorService.shutdownNow();
                IOUtils.closeQuietly(serverSocket);
                log.info("Metric server stopped");
            }
        }));
        log.info("Metric server started");
    }

    private class MetricServerWorker implements Runnable {

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    read();
                } catch (Exception e) {
                    log.warn("Failed to read data", e);
                }
            }
        }

        private void read() throws IOException {
            Date currentDate = new Date();
            try (Socket socket = serverSocket.accept()) {
                socket.setSoTimeout(socketTimeoutMillis);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    Metric metric = createMetric(line, currentDate);
                    if (metric != null) {
                        metricCacher.submitMetric(metric);
                    }
                }
            }
        }
    }

    private Metric createMetric(String line, Date currentDate) {
        String[] splits = line.split(" ");
        if (splits.length != 3) {
            return null;
        }
        String name = splits[0];
        if (!MetricValidator.validate(name)) {
            return null;
        }
        try {
            double value = Double.parseDouble(splits[1]);
            int time = Integer.valueOf(splits[2]);
            if (time <= 0) {
                return null;
            }
            MetricStatus status = metricSearch.add(name);
            if (status == MetricStatus.NEW || status == MetricStatus.EXISTING) {
                return new Metric(name, time, value, currentDate);
            } else {
                return null;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Required
    public void setMetricSearch(MetricSearch metricSearch) {
        this.metricSearch = metricSearch;
    }

    @Required
    public void setMetricCacher(MetricCacher metricCacher) {
        this.metricCacher = metricCacher;
    }

    public void setSocketTimeoutMillis(int socketTimeoutMillis) {
        this.socketTimeoutMillis = socketTimeoutMillis;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
