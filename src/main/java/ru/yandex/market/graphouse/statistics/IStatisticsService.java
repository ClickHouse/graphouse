package ru.yandex.market.graphouse.statistics;

import java.util.function.Supplier;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 25.12.17
 */
public interface IStatisticsService {
    void accumulateMetric(AccumulatedMetric metric, double value);

    void registerInstantMetric(InstantMetric metric, Supplier<Double> supplier);
}
