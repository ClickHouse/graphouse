package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 09/11/2018
 */
public class NotificationMetric extends MetricBase{

    public NotificationMetric(MetricDir parent, String name) {
        super(parent, name, MetricStatus.APPROVED);
    }

    @Override
    public String getName() {
        return parent.isRoot() ? name : parent.getName() + name;
    }

    @Override
    public boolean isDir() {
        return false;
    }
}
