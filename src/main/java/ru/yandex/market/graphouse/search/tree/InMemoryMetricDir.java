package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 26/01/2017
 */
public class InMemoryMetricDir extends MetricDir {
    private volatile Map<String, MetricName> metrics;
    private volatile Map<String, MetricDir> dirs;

    public InMemoryMetricDir(MetricDir parent, String name, MetricStatus status) {
        super(parent, name, status);
    }

    private void initDirs() {
        if (dirs == null) {
            synchronized (this) {
                if (dirs == null) {
                    dirs = new ConcurrentHashMap<>();
                }
            }
        }
    }

    private void initMetrics() {
        if (metrics == null) {
            synchronized (this) {
                if (metrics == null) {
                    metrics = new ConcurrentHashMap<>();
                }
            }
        }
    }

    @Override
    public MetricDir maybeGetDir(String name) {
        if (dirs == null){
            return null;
        }
        return dirs.get(name);
    }

    @Override
    public MetricName maybeGetMetric(String name) {
        if (metrics == null){
            return null;
        }
        return metrics.get(name);
    }

    @Override
    public Map<String, MetricName> getMetrics() {
        initMetrics();
        return metrics;
    }

    @Override
    public Map<String, MetricDir> getDirs() {
        initDirs();
        return dirs;
    }

    public boolean hasDirs() {
        return dirs != null && !dirs.isEmpty();
    }

    public boolean hasMetrics() {
        return metrics != null && !metrics.isEmpty();
    }


    public int loadedMetricCount() {
        int count = 0;

        if (hasMetrics()) {
            count = metrics.size();
        }

        if (hasDirs()) {
            for (MetricDir dir : dirs.values()) {
                count += dir.loadedMetricCount();
            }
        }
        return count;
    }

    public int loadedDirCount() {
        if (!hasDirs()) {
            return 0;
        }

        int count = dirs.size();
        for (MetricDir dir : dirs.values()) {
            count += dir.loadedDirCount();
        }
        return count;
    }
}
