package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public abstract class MetricBase implements MetricDescription {
    protected final MetricDir parent;
    protected final String name;

    private volatile long updateTimeMillis = System.currentTimeMillis();
    private volatile MetricStatus status = MetricStatus.SIMPLE;

    MetricBase(MetricDir parent, String name, MetricStatus status) {
        this.parent = parent;
        this.name = name;
        this.status = status;
    }

    public boolean visible() {
        return status.visible();
    }

    public boolean isRoot() {
        return parent == null;
    }

    @Override
    public MetricStatus getStatus() {
        return status;
    }

    public void setStatus(MetricStatus newStatus) {
        newStatus = MetricTree.selectStatus(status, newStatus);
        if (status != newStatus) {
            MetricStatus oldStatus = status;
            status = newStatus;
            updateTimeMillis = System.currentTimeMillis();
            parent.notifyChildStatusChange(this, oldStatus);
        }
    }

    public int getLevel() {
        int level = 0;
        MetricBase metricBase = this;
        while (!metricBase.isRoot()) {
            level++;
            metricBase = metricBase.getParent();
        }
        return level;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public long getUpdateTimeMillis() {
        return updateTimeMillis;
    }

    @Override
    public MetricDir getParent() {
        return parent;
    }

}
