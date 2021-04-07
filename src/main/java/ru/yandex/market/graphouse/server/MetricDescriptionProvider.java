package ru.yandex.market.graphouse.server;

import ru.yandex.market.graphouse.search.tree.MetricDescription;

public interface MetricDescriptionProvider {
    MetricDescription getMetricDescription(String name);
}
