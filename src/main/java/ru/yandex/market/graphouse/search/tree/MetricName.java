package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.retention.MetricRetention;
import ru.yandex.market.graphouse.search.retention.RetentionProvider;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public class MetricName extends MetricBase {

    private final MetricRetention retention;

    public MetricName(MetricDir parent, String name, MetricStatus status, RetentionProvider retentionProvider) {
        this(parent, name, status, retentionProvider.getRetention(name));
    }

    public MetricName(MetricDir parent, String name, MetricStatus status, MetricRetention retention) {
        super(parent, name, status);
        this.retention = retention;
    }

    @Override
    public boolean isDir() {
        return false;
    }

    @Override
    public String getName() {
        return parent.isRoot() ? name : parent.toString() + name;
    }

    public MetricRetention getRetention() {
        return retention;
    }
}
