package ru.yandex.market.graphouse.search.tree;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 26/01/2017
 */
public class DirContent {

    public static final DirContent EMPTY = new DirContent();

    public DirContent(ConcurrentMap<String, MetricDir> dirs, ConcurrentMap<String, MetricName> metrics) {
        this.dirs = dirs;
        this.metrics = metrics;
    }

    private DirContent() {
        this.dirs = Collections.emptyMap();
        this.metrics = Collections.emptyMap();
    }

    private final Map<String, MetricDir> dirs;
    private final Map<String, MetricName> metrics;

    public Map<String, MetricDir> getDirs() {
        return dirs;
    }

    public Map<String, MetricName> getMetrics() {
        return metrics;
    }

    public static DirContent createEmpty() {
        return new DirContent(new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
    }
}
