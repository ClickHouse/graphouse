package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public class MetricName extends MetricBase {

    public MetricName(MetricDir parent, String name, MetricStatus status) {
        super(parent, name, status);
    }

    @Override
    public boolean isDir() {
        return false;
    }

    @Override
    public String getName() {
        return parent.isRoot() ? name : parent.toString() + name;
    }
}
