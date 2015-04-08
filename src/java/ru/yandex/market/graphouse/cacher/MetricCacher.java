package ru.yandex.market.graphouse.cacher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.clickhouse.ClickhouseTemplate;
import ru.yandex.market.graphouse.Metric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 06/04/15
 */
public class MetricCacher implements Runnable, InitializingBean {

    private static final Logger log = LogManager.getLogger();

    private int cacheSize = 2_000_000;
    private int batchSize = 200_000;
    private int writersCount = 10;
    private int flushIntervalSeconds = 5;

    private Queue<Metric> metricQueue;
    private AtomicInteger activeWriters = new AtomicInteger(0);

    private ClickhouseTemplate clickhouseTemplate;

    private ExecutorService executorService;

    @Override
    public void afterPropertiesSet() throws Exception {
        metricQueue = new ArrayBlockingQueue<>(cacheSize);
        executorService = Executors.newFixedThreadPool(writersCount);
        new Thread(this, "Metric cacher thread").start();
    }

    public void submitMetric(Metric metric) {
        metricQueue.add(metric);
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
        if (metricQueue.isEmpty()){
            return;
        }
        log.info("Metric queue size: " + metricQueue.size() + ", active writers: " + activeWriters.get());
        //TODO мониторинг, если очередь слишком большая
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
                    ok = true;
                } catch (Exception e) {
                    log.error("Failed to save metrics.", e);
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
