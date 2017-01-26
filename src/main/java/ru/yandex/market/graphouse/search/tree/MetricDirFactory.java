package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public interface MetricDirFactory {
    MetricDir createMetricDir(MetricDir parent, String name, MetricStatus status);
}
