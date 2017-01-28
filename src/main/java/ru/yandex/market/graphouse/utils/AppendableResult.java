package ru.yandex.market.graphouse.utils;

import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.io.IOException;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 08.11.16
 */
public interface AppendableResult {

    void appendMetric(MetricDescription metric) throws IOException;

}
