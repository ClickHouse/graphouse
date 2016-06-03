package ru.yandex.market.graphouse.cacher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.clickhouse.ClickhouseTemplate;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.monitoring.ComplicatedMonitoring;
import ru.yandex.market.monitoring.MonitoringUnit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 06/04/15
 */
public class MetricCacher implements Runnable, InitializingBean {

    private static final Logger log = LogManager.getLogger();

    private ClickhouseTemplate clickhouseTemplate;
    private ComplicatedMonitoring monitoring;


    private int cacheSize = 1_000_000;
    private int batchSize = 1_000_000;
    private int writersCount = 2;
    private int flushIntervalSeconds = 5;

    private MonitoringUnit metricCacherQueryUnit = new MonitoringUnit("MetricCacherQueue");
    private final Semaphore semaphore = new Semaphore(0, false);
    private BlockingQueue<Metric> metricQueue;
    private AtomicInteger activeWriters = new AtomicInteger(0);


    private ExecutorService executorService;

    @Override
    public void afterPropertiesSet() throws Exception {
        metricQueue = new ArrayBlockingQueue<>(cacheSize);
        semaphore.release(cacheSize);
        executorService = Executors.newFixedThreadPool(writersCount);
        monitoring.addUnit(metricCacherQueryUnit);
        new Thread(this, "Metric cacher thread").start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Shutting down metric cacher. Saving all cached metrics...");
                while (!metricQueue.isEmpty()) {
                    log.info(metricQueue.size() + " metrics remaining");
                    createBatches();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                }
                executorService.shutdown();
                while (!executorService.isTerminated()) {
                    log.info("Awaiting save completion");
                    try {
                        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ignored) {
                    }
                }
                log.info("Metric cacher stopped");
            }
        }));
    }

    public void submitMetric(Metric metric) {
        try {
            semaphore.acquire();
            metricQueue.put(metric);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(flushIntervalSeconds));
                createBatches();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void createBatches() {
        if (metricQueue.isEmpty()) {
            metricCacherQueryUnit.ok();
            return;
        }
        int queueSize = cacheSize - semaphore.availablePermits();
        double queueOccupancyPercent = queueSize * 100.0 / cacheSize;
        log.info(
            "Metric queue size: " + queueSize + "(" + queueOccupancyPercent + "%)" +
                ", active writers: " + activeWriters.get()
        );
        queueSizeMonitoring(queueOccupancyPercent);
        while (activeWriters.get() < writersCount) {
            List<Metric> metrics = createBatch();
            if (metrics.isEmpty()) {
                return;
            }
            executorService.submit(new ClickhouseWriterWorker(metrics));
            activeWriters.incrementAndGet();
            if (metrics.size() < batchSize) {
                return; //Не набрали полный батч, значит больше и не надо
            }
        }
    }

    private void queueSizeMonitoring(double queueOccupancyPercent) {
        if (queueOccupancyPercent >= 95) {
            metricCacherQueryUnit.critical("Metric queue size >= 95%");
        } else if (queueOccupancyPercent >= 80) {
            metricCacherQueryUnit.warning("Metric queue size >= 80%");
        } else {
            metricCacherQueryUnit.ok();
        }
    }

    private List<Metric> createBatch() {
        List<Metric> metrics = new ArrayList<>(Math.min(batchSize, metricQueue.size()));
        for (int i = 0; i < batchSize; i++) {
            Metric metric = metricQueue.poll();
            if (metric == null) {
                break;
            }
            metrics.add(metric);
        }
        return metrics;
    }

    private class ClickhouseWriterWorker implements Runnable {

        private List<Metric> metrics = new ArrayList<>();

        public ClickhouseWriterWorker(List<Metric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public void run() {
            boolean ok = false;
            while (!ok) {
                try {
                    long start = System.currentTimeMillis();
                    saveMetrics();
                    long processed = System.currentTimeMillis() - start;
                    log.info("Saved " + metrics.size() + " metrics in " + processed + "ms");
                    semaphore.release(metrics.size());
                    ok = true;
                } catch (Exception e) {
                    log.error("Failed to save metrics. Waiting 1 second before retry", e);
                    monitoring.addTemporaryCritical(
                        "MetricCacherClickhouseOutput", e.getMessage(), 1, TimeUnit.MINUTES
                    );
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
            activeWriters.decrementAndGet();
        }

        private void saveMetrics() throws IOException {
            MetricInputStream metricInputStream = new MetricInputStream(metrics);
            clickhouseTemplate.sendStream(metricInputStream, "graphite");
        }
    }

    @Required
    public void setClickhouseTemplate(ClickhouseTemplate clickhouseTemplate) {
        this.clickhouseTemplate = clickhouseTemplate;
    }

    @Required
    public void setMonitoring(ComplicatedMonitoring monitoring) {
        this.monitoring = monitoring;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setWritersCount(int writersCount) {
        this.writersCount = writersCount;
    }

    public void setFlushIntervalSeconds(int flushIntervalSeconds) {
        this.flushIntervalSeconds = flushIntervalSeconds;
    }
}
