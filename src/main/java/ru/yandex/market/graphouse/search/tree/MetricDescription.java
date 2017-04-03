package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.retention.MetricRetention;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 02/09/15
 */
public interface MetricDescription {

    String getName();

    MetricStatus getStatus();

    boolean isDir();

    long getUpdateTimeMillis();

    MetricDescription getParent();

    int getLevel();

    boolean isRoot();

}
