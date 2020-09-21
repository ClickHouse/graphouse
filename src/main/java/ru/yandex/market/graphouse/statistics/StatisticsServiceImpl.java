package ru.yandex.market.graphouse.statistics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 22.12.17
 */
public class StatisticsServiceImpl implements StatisticsService {
    private static final Logger log = LogManager.getLogger();
    private List<StatisticsCounter> counters;
    private ScheduledExecutorService scheduler;
    private final Map<InstantMetric, Supplier<Double>> instantMetricsSuppliers = new ConcurrentHashMap<>();

    public void initialize(List<StatisticsCounter> counters) {
        this.scheduler = Executors.newScheduledThreadPool(counters.size());

        this.counters = counters;
        this.counters.forEach(StatisticsCounter::initialize);
        this.counters.forEach(this::scheduleStatisticsCounter);
    }

    private void scheduleStatisticsCounter(StatisticsCounter counter) {
        this.scheduler.scheduleAtFixedRate(
            () -> counter.flush(this.instantMetricsSuppliers),
            counter.getFlushPeriodSeconds(),
            counter.getFlushPeriodSeconds(),
            TimeUnit.SECONDS
        );
    }

    @Override
    public void shutdownService() {
        log.info("Shutting down statistics service");
        scheduler.shutdown();
        log.info("Statistics service stopped");
    }

    @Override
    public void accumulateMetric(AccumulatedMetric metric, double value) {
        this.counters.forEach(x -> x.accumulateMetric(metric, value));
    }

    @Override
    public void registerInstantMetric(InstantMetric metric, Supplier<Double> supplier) {
        this.instantMetricsSuppliers.put(metric, supplier);
    }
}
