package ru.yandex.market.graphouse.statistics;

import java.util.concurrent.atomic.AtomicInteger;

public class LoadedMetricsCounter {
    private final StatisticsService statisticsService;

    private final AtomicInteger numberOfLoadedDirs = new AtomicInteger();
    private final AtomicInteger numberOfLoadedMetrics = new AtomicInteger();

    public LoadedMetricsCounter(StatisticsService statisticsService) {
        this.statisticsService = statisticsService;

        this.statisticsService.registerInstantMetric(InstantMetric.NUMBER_OF_LOADED_DIRS,
            () -> (double) numberOfLoadedDirs.get());
        this.statisticsService.registerInstantMetric(InstantMetric.NUMBER_OF_LOADED_METRICS,
            () -> (double) numberOfLoadedMetrics.get());
    }

    public void addLoadedDirs(int count) {
        numberOfLoadedDirs.addAndGet(count);
        statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_LOADED_DIRS, count);
    }

    public void addLoadedMetrics(int count) {
        numberOfLoadedMetrics.addAndGet(count);
        statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_LOADED_METRICS, count);
    }
}
