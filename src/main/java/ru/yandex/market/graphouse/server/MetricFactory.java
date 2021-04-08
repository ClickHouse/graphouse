package ru.yandex.market.graphouse.server;

import ru.yandex.market.graphouse.Metric;

public interface MetricFactory {
    Metric createMetric(String line, int updatedSeconds);
}
