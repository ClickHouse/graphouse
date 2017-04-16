package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.retention.MetricRetention;
import ru.yandex.market.graphouse.retention.RetentionProvider;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public class MetricName extends MetricBase {

    private final MetricRetention retention;

    public MetricName(MetricDir parent, String name, MetricStatus status, RetentionProvider retentionProvider) {
        this(parent, name, status, retentionProvider.getRetention(parent.getName() + name));
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
        return parent.isRoot() ? name : parent.getName() + name;
    }

    public MetricRetention getRetention() {
        return retention;
    }
}
