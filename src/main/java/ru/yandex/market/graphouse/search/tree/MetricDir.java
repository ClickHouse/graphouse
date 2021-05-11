package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.retention.RetentionProvider;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public abstract class MetricDir extends MetricBase {
    private static final String ROOT_NAME = "ROOT";

    private final AtomicInteger visibleChildren = new AtomicInteger(0);

    public MetricDir(MetricDir parent, String name, MetricStatus status) {
        super(parent, name, status);
    }

    @Override
    public boolean isDir() {
        return true;
    }

    @Override
    public String getName() {
        if (isRoot()) {
            return ROOT_NAME;
        }

        final String dirName = name + MetricUtil.LEVEL_SPLITTER;
        return parent.isRoot() ? dirName : parent.toString() + dirName;
    }

    public abstract Map<String, MetricDir> maybeGetDirs();

    public abstract Map<String, MetricDir> getDirs();

    public abstract Map<String, MetricName> getMetrics();

    public abstract Map<String, MetricName> maybeGetMetrics();

    public abstract boolean hasDirs();

    public abstract boolean hasMetrics();

    public abstract int loadedMetricCount();

    public abstract int loadedDirCount();

    public abstract MetricDir maybeGetDir(String name);

    public abstract MetricName maybeGetMetric(String name);

    public MetricDir getOrCreateDir(String name, MetricStatus status,
                                    MetricDirFactory metricDirFactory, int maxSubDirsPerDir) {
        Map<String, MetricDir> dirs = getDirs();
        MetricDir dir = dirs.get(name);
        if (dir != null) {
            return dir;
        }
        if (maxSubDirsPerDir > 0 && dirs.size() >= maxSubDirsPerDir && !status.handmade()) {
            return null;
        }
        String internName = name.intern();
        dir = dirs.computeIfAbsent(
            internName,
            s -> metricDirFactory.createMetricDir(MetricDir.this, internName, status)
        );
        notifyChildStatusChange(dir, null, dir.getStatus()); //Can be false call, but its ok
        return dir;
    }

    public MetricName getOrCreateMetric(String name, MetricStatus status,
                                        RetentionProvider retentionProvider, int maxMetricsPerDir) {
        Map<String, MetricName> metrics = getMetrics();
        MetricName metric = metrics.get(name);
        if (metric != null) {
            return metric;
        }
        if (maxMetricsPerDir > 0 && metrics.size() >= maxMetricsPerDir && !status.handmade()) {
            return null;
        }
        String internName = name.intern();
        metric = metrics.computeIfAbsent(
            internName,
            s -> new MetricName(this, internName, status, retentionProvider.getRetention(getName() + name))
        );
        notifyChildStatusChange(metric, null, metric.getStatus()); //Can be false call, but its ok
        return metric;
    }


    /**
     * if all the metrics in the directory are hidden, then we try to hide it {@link MetricStatus#AUTO_HIDDEN}.
     * if there is at least one open metric for the directory, then we try to open it {@link MetricStatus#SIMPLE}.
     *
     * @param metricBase  - updated child metric
     * @param oldStatus   - metricBase old status, may be null
     * @param eventStatus - new status triggered event
     */
    public void notifyChildStatusChange(MetricBase metricBase, MetricStatus oldStatus, MetricStatus eventStatus) {
        if (isRoot()) {
            return;
        }

        MetricStatus newStatus = metricBase.getStatus();

        // remove from the tree, it does not make sense to store it
        if (newStatus == MetricStatus.AUTO_HIDDEN) {
            if (metricBase.isDir()) {
                getDirs().remove(metricBase.getName());
            } else {
                getMetrics().remove(metricBase.getName());
            }
        }

        if (oldStatus != null && oldStatus.visible() == newStatus.visible()) {
            if (eventStatus.visible()) {
                tryOpenHiddenDirectories(eventStatus);
            }
            return;
        }

        // if all the metrics in the directory are hidden, then we try to hide it {@link MetricStatus#AUTO_HIDDEN}
        // if there is at least one open metric for the directory, then we try to open it {@link MetricStatus#SIMPLE}
        if (newStatus.visible()) {
            setStatus(MetricStatus.SIMPLE, eventStatus);
            visibleChildren.getAndIncrement();
        } else {
            visibleChildren.getAndUpdate(operand -> {
                int count;
                if (operand <= 1) {
                    count = calculateVisibleCounter();
                } else {
                    count = operand - 1;
                }
                setStatus(count > 0 ? MetricStatus.SIMPLE : MetricStatus.AUTO_HIDDEN, eventStatus);
                return count;
            });
        }
    }

    private void tryOpenHiddenDirectories(MetricStatus eventStatus) {
        // if some metric change status to visible, then we try to open all parent directories
        MetricStatus currentStatus = getStatus();
        switch (currentStatus) {
            case BAN:
            case AUTO_BAN:
                break;
            case HIDDEN:
            case AUTO_HIDDEN:
                setStatus(MetricStatus.SIMPLE, eventStatus);
                break;
            default:
                parent.notifyChildStatusChange(this, currentStatus, eventStatus);
        }
    }

    private int calculateVisibleCounter() {
        return (int) Stream.concat(getDirs().values().stream(), getMetrics().values().stream())
            .filter(MetricBase::visible)
            .count();
    }
}
