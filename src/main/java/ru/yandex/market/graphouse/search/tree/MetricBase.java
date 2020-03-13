package ru.yandex.market.graphouse.search.tree;

import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public abstract class MetricBase implements MetricDescription {
    private static final byte LEVEL_SPLITTER = (byte) MetricUtil.LEVEL_SPLITTER;

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
    public void writeName(ClickHouseRowBinaryStream stream) throws IOException {
        if (!parent.isRoot()) {
            parent.writeName(stream);
        }
        stream.writeBytes(name.getBytes());
        if (isDir()) {
            stream.writeByte(LEVEL_SPLITTER);
        }
    }

    @Override
    public int getNameLengthInBytes() {
        int length = name.getBytes().length;
        if (isDir()) {
            length++;
        }
        if (!parent.isRoot()) {
            length += parent.getNameLengthInBytes();
        }
        return length;
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
        return "MetricBase{" +
            "parent=" + parent +
            ", name='" + name + '\'' +
            ", updateTimeMillis=" + updateTimeMillis +
            ", status=" + status +
            ", visible=" + status.visible() +
            '}';
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
