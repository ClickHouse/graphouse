package ru.yandex.market.graphouse.cacher;

import com.google.common.base.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;
import ru.yandex.clickhouse.ClickHouseStatementImpl;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.monitoring.MonitoringUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 06/04/15
 */
public class MetricCacher implements Runnable, InitializingBean {

    private static final Logger log = LogManager.getLogger();

    private final JdbcTemplate clickHouseJdbcTemplate;
    private final Monitoring monitoring;

    @Value("${graphouse.clickhouse.data-write-table}")
    private String graphiteDataWriteTable;

    @Value("${graphouse.cacher.queue-size}")
    private int queueSize = 1_000_000;

    @Value("${graphouse.cacher.min-batch-size}")
    private int minBatchSize = 10_000;

    @Value("${graphouse.cacher.max-batch-size}")
    private int maxBatchSize = 500_000;

    @Value("${graphouse.cacher.min-batch-time-seconds}")
    private int minBatchTimeSeconds = 1;

    @Value("${graphouse.cacher.max-batch-time-seconds}")
    private int maxBatchTimeSeconds = 5;

    @Value("${graphouse.cacher.max-output-threads}")
    private int maxOutputThreads = 2;

    private final AtomicLong lastBatchTimeMillis = new AtomicLong(System.currentTimeMillis());
    private final MonitoringUnit metricCacherQueryUnit = new MonitoringUnit("MetricCacherQueue", 2, TimeUnit.MINUTES);
    private final Semaphore semaphore = new Semaphore(0, false);
    private BlockingQueue<Metric> metricQueue;
    private final AtomicInteger activeWriters = new AtomicInteger(0);
    private final AtomicInteger activeOutputMetrics = new AtomicInteger(0);

    private ExecutorService executorService;

    public MetricCacher(JdbcTemplate clickHouseJdbcTemplate, Monitoring monitoring) {
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.monitoring = monitoring;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        metricQueue = new ArrayBlockingQueue<>(queueSize);
        semaphore.release(queueSize);
        executorService = Executors.newFixedThreadPool(maxOutputThreads);
        monitoring.addUnit(metricCacherQueryUnit);
        new Thread(this, "Metric cacher thread").start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down metric cacher. Saving all cached metrics...");
            while (!(metricQueue.isEmpty() && (activeOutputMetrics.get() == 0))) {
                log.info((metricQueue.size() + activeOutputMetrics.get()) + " metrics remaining");
                createBatches(true);
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

    public void submitMetrics(List<Metric> metrics) {
        if (metrics.isEmpty()) {
            return;
        }
        try {
            semaphore.acquire(metrics.size());
            metricQueue.addAll(metrics);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(1);
                createBatches(false);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private boolean needBatch() {
        if (metricQueue.size() >= maxBatchSize) {
            return true;
        }
        long secondsPassed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - lastBatchTimeMillis.get());
        if (secondsPassed >= maxBatchTimeSeconds) {
            return true;
        }
        if (metricQueue.size() >= minBatchSize && secondsPassed >= minBatchTimeSeconds) {
            return true;
        }
        return false;
    }

    private void createBatches(boolean force) {
        if (metricQueue.isEmpty()) {
            metricCacherQueryUnit.ok();
            return;
        }
        int queueSize = this.queueSize - semaphore.availablePermits();
        double queueOccupancyPercent = queueSize * 100.0 / this.queueSize;
        queueSizeMonitoring(queueOccupancyPercent);
        int createdBatches = 0;
        int metricsInBatches = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while ((needBatch() || force) && activeWriters.get() < maxOutputThreads) {
            List<Metric> metrics = createBatch();
            if (metrics.isEmpty()) {
                continue;
            }
            createdBatches++;
            metricsInBatches += metrics.size();
            executorService.submit(new ClickhouseWriterWorker(metrics));
            activeWriters.incrementAndGet();
            activeOutputMetrics.addAndGet(metrics.size());
            lastBatchTimeMillis.set(System.currentTimeMillis());
        }
        stopwatch.stop();
        if (createdBatches > 0) {
            log.info(
                "Created " + createdBatches + " output worker(s) (" + activeWriters.get() + " total) " +
                    "for " + metricsInBatches + " metrics (" + activeOutputMetrics.get() + " total in processing) " +
                    "in " + stopwatch.toString() + ". " +
                    "Metric queue size: " + queueSize + " (" + queueOccupancyPercent + "%)");
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
        int batchSize = Math.min(maxBatchSize, metricQueue.size());
        List<Metric> metrics = new ArrayList<>(batchSize);
        metricQueue.drainTo(metrics, batchSize);
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
            activeOutputMetrics.addAndGet(-metrics.size());
            activeWriters.decrementAndGet();
        }

        private void saveMetrics() {
            clickHouseJdbcTemplate.execute(
                (StatementCallback<Void>) stmt -> {
                    ClickHouseStatementImpl statement = (ClickHouseStatementImpl) stmt;
                    MetricsStreamCallback metricsStreamCallback = new MetricsStreamCallback(
                        metrics, statement.getConnection().getTimeZone()
                    );
                    statement.sendRowBinaryStream(
                        "INSERT INTO " + graphiteDataWriteTable + " (metric, value, timestamp, date, updated)",
                        metricsStreamCallback
                    );
                    return null;
                }
            );
        }
    }
}
