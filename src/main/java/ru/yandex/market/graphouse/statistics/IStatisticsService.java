package ru.yandex.market.graphouse.statistics;

import java.util.function.Supplier;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 25.12.17
 */
public interface IStatisticsService {
    /**
     * Accumulates metric
     *
     * @param metric
     * @param value
     */
    void accumulateMetric(AccumulatedMetric metric, double value);

    /**
     * Registers instant metric supplier
     *
     * @param metric
     * @param supplier
     */
    void registerInstantMetric(InstantMetric metric, Supplier<Double> supplier);
}
