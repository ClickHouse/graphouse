package ru.yandex.market.graphouse.statistics;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 22.12.17
 */
public class StatisticsService implements IStatisticsService {
    private final List<StatisticsCounter> counters;
    private final ScheduledExecutorService scheduler;

    public StatisticsService(List<StatisticsCounter> counters, int numberOfThreads) {
        this.counters = counters;
        this.scheduler = Executors.newScheduledThreadPool(numberOfThreads);
    }

    @PostConstruct
    public void initialize() {
        this.counters.forEach(StatisticsCounter::initialize);
        this.counters.forEach(agent -> this.scheduler.scheduleAtFixedRate(
            agent::flush, agent.getFlushPeriodSeconds(), agent.getFlushPeriodSeconds(), TimeUnit.SECONDS)
        );
    }

    @Override
    public void accumulateMetric(AccumulatedMetric metric, double value) {
        this.counters.forEach(x -> x.accumulateMetric(metric, value));
    }
}
