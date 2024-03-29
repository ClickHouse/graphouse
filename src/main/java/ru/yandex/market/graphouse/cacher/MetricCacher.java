package ru.yandex.market.graphouse.cacher;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import ru.yandex.market.graphouse.statistics.AccumulatedMetric;
import ru.yandex.market.graphouse.statistics.InstantMetric;
import ru.yandex.market.graphouse.statistics.StatisticsService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
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
    private final StatisticsService statisticsService;

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

    @Value("${graphouse.cacher.min-retry-millis}")
    private int retryMillis = 1000;

    @Value("${graphouse.cacher.retry-interval-millis}")
    private int retryIntervalMillis = 3000;

    private final AtomicLong lastBatchTimeMillis = new AtomicLong(System.currentTimeMillis());
    private final MonitoringUnit metricCacherQueryUnit = new MonitoringUnit("MetricCacherQueue", 2, TimeUnit.MINUTES);
    private final Semaphore semaphore = new Semaphore(0, false);
    private BlockingQueue<Metric> metricQueue;
    private final AtomicInteger activeWriters = new AtomicInteger(0);
    private final AtomicInteger activeOutputMetrics = new AtomicInteger(0);
    private volatile boolean shutdown = false;

    private ExecutorService executorService;

    public MetricCacher(JdbcTemplate clickHouseJdbcTemplate, Monitoring monitoring,
                        StatisticsService statisticsService) {
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.monitoring = monitoring;
        this.statisticsService = statisticsService;

        statisticsService.registerInstantMetric(
            InstantMetric.METRIC_CACHE_QUEUE_SIZE, () -> (double) this.metricQueue.size()
        );

        statisticsService.registerInstantMetric(
            InstantMetric.METRIC_CACHE_QUEUE_LOAD_PERCENT, this::getQueueOccupancyPercent
        );
    }

    @Override
    public void afterPropertiesSet() {
        metricQueue = new ArrayBlockingQueue<>(queueSize);
        semaphore.release(queueSize);
        executorService = Executors.newFixedThreadPool(
            maxOutputThreads,
            new ThreadFactoryBuilder().setNameFormat("output-thread-%d").build()
        );
        monitoring.addUnit(metricCacherQueryUnit);
        new Thread(this, "Metric cacher thread").start();
    }

    public void flushAndShutdown() {
        log.info("Shutting down metric cacher. Saving all cached metrics...");
        shutdown = true;
        flushOutputMetrics();
        executorService.shutdown();
        statisticsService.shutdownService();
        awaitSaveCompletion();
        log.info("Metric cacher stopped");
    }

    private void flushOutputMetrics() {
        while (hasOutputMetrics()) {
            log.info((metricQueue.size() + activeOutputMetrics.get()) + " metrics remaining");
            createBatches(true);
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void awaitSaveCompletion() {
        while (!executorService.isTerminated()) {
            log.info("Awaiting save completion");
            try {
                executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private boolean hasOutputMetrics() {
        return !(metricQueue.isEmpty() && (activeOutputMetrics.get() == 0));
    }

    public void submitMetric(Metric metric) {
        try {
            semaphore.acquire();
            metricQueue.put(metric);
            statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_RECEIVED_METRICS, 1);
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
            statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_RECEIVED_METRICS, metrics.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                createBatches(false);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private boolean needBatch(boolean force) {
        if (force) {
            return !metricQueue.isEmpty();
        } else if (shutdown) {
            return false;
        }

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

    private double getQueueOccupancyPercent() {
        int currentQueueSize = this.queueSize - semaphore.availablePermits();
        return currentQueueSize * 100.0 / this.queueSize;
    }

    private void createBatches(boolean force) {
        if (metricQueue.isEmpty()) {
            metricCacherQueryUnit.ok();
            return;
        }

        double queueOccupancyPercent = getQueueOccupancyPercent();
        queueSizeMonitoring(queueOccupancyPercent);

        int createdBatches = 0;
        int metricsInBatches = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();

        while (needBatch(force) && activeWriters.get() < maxOutputThreads) {
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
        private List<Metric> metrics;

        ClickhouseWriterWorker(List<Metric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public void run() {
            while (!trySaveMetrics()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(
                        retryMillis + ThreadLocalRandom.current().nextInt(retryIntervalMillis)
                    );
                } catch (InterruptedException ignored) {
                }
            }

            activeOutputMetrics.addAndGet(-metrics.size());
            activeWriters.decrementAndGet();
            statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_WRITTEN_METRICS, metrics.size());
        }

        private boolean trySaveMetrics() {
            try {
                long start = System.currentTimeMillis();
                saveMetrics();
                long processed = System.currentTimeMillis() - start;

                semaphore.release(metrics.size());

                log.info(String.format("Saved %d metrics in %d ms", metrics.size(), processed));
            } catch (Exception e) {
                log.error("Failed to save metrics. Waiting " + retryMillis + " millis before retry", e);

                statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_WRITE_ERRORS, 1);

                monitoring.addTemporaryCritical(
                    "MetricCacherClickhouseOutput", e.getMessage(), 1, TimeUnit.MINUTES
                );

                return false;
            }

            return true;
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
