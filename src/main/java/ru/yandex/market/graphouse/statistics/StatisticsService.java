package ru.yandex.market.graphouse.statistics;

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
public class StatisticsService implements IStatisticsService {
    private List<StatisticsCounter> counters;
    private final ScheduledExecutorService scheduler;
    private final Map<InstantMetric, Supplier<Double>> instantMetricsSuppliers = new ConcurrentHashMap<>();

    public StatisticsService(int numberOfThreads) {
        this.scheduler = Executors.newScheduledThreadPool(numberOfThreads);
    }

    public void initialize(List<StatisticsCounter> counters) {
        this.counters = counters;
        this.counters.forEach(StatisticsCounter::initialize);
        this.counters.forEach(counter ->
            this.scheduler.scheduleAtFixedRate(
                () -> counter.flush(this.instantMetricsSuppliers), counter.getFlushPeriodSeconds(),
                counter.getFlushPeriodSeconds(), TimeUnit.SECONDS
            )
        );
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
