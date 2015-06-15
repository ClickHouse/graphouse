package ru.yandex.market.graphouse.search;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 07/04/15
 */
public class MetricTree {

    private final Dir root = new Dir(null, "");

    public void search(String query, Appendable result) throws IOException {
        String[] levels = query.split("\\.");
        search(root, levels, 0, result);
    }


    private void search(Dir parentDir, String[] levels, int levelIndex, Appendable result) throws IOException {
        if (!parentDir.status.visible()) {
            return;
        }
        boolean isLast = (levelIndex == levels.length - 1);
        String level = levels[levelIndex];
        boolean isPattern = containsExpressions(level);

        if (!isPattern) {
            if (isLast) {
                appendSimpleResult(parentDir, level, result);
            } else {
                Dir dir = parentDir.dirs.get(level);
                if (dir != null) {
                    search(dir, levels, levelIndex + 1, result);
                }
            }
        } else if (level.equals("*")) {
            if (isLast) {
                appendAllResult(parentDir, result);
            } else {
                for (Dir dir : parentDir.dirs.values()) {
                    search(dir, levels, levelIndex + 1, result);
                }
            }
        } else {
            Pattern pattern = createPattern(level);
            if (isLast) {
                appendPatternResult(parentDir, pattern, result);
            } else {
                for (Map.Entry<String, Dir> dirEntry : parentDir.dirs.entrySet()) {
                    if (pattern.matcher(dirEntry.getKey()).matches()) {
                        search(dirEntry.getValue(), levels, levelIndex + 1, result);
                    }
                }
            }
        }
    }

    private Pattern createPattern(String globPattern) {
        globPattern = globPattern.replace("*", "[-_0-9a-zA-Z]*");
        globPattern = globPattern.replace("?", "[-_0-9a-zA-Z]");
        return Pattern.compile(globPattern);
    }

    private void appendSimpleResult(Dir parentDir, String name, Appendable result) throws IOException {
        appendResult(parentDir.dirs.get(name), result);
        appendResult(parentDir.metrics.get(name), result);
    }

    private void appendAllResult(Dir parentDir, Appendable result) throws IOException {
        for (Dir dir : parentDir.dirs.values()) {
            appendResult(dir, result);
        }
        for (MetricName metric : parentDir.metrics.values()) {
            appendResult(metric, result);
        }
    }

    private void appendPatternResult(Dir parentDir, Pattern pattern, Appendable result) throws IOException {
        for (Map.Entry<String, Dir> dirEntry : parentDir.dirs.entrySet()) {
            Dir dir = dirEntry.getValue();
            if (dir.status.visible() && pattern.matcher(dirEntry.getKey()).matches()) {
                appendResult(dir, result);
            }
        }
        for (Map.Entry<String, MetricName> metricEntry : parentDir.metrics.entrySet()) {
            MetricName metricName = metricEntry.getValue();
            if (metricName.status.visible() && pattern.matcher(metricEntry.getKey()).matches()) {
                appendResult(metricName, result);
            }
        }
    }

    private void appendResult(Dir dir, Appendable result) throws IOException {
        if (dir != null && dir.status.visible()) {
            appendDir(dir, result);
            result.append('\n');
        }
    }

    private void appendResult(MetricName metric, Appendable result) throws IOException {
        if (metric != null && metric.status.visible()) {
            appendDir(metric.parent, result);
            result.append(metric.name).append('\n');
        }
    }

    private void appendDir(Dir dir, Appendable result) throws IOException {
        if (dir.isRoot()) {
            return;
        }
        appendDir(dir.parent, result);
        result.append(dir.name).append('.');
    }

    public QueryStatus add(String metric, MetricStatus status) {
        return modify(metric, status);
    }

    public QueryStatus add(String metric) {
        return add(metric, MetricStatus.SIMPLE);
    }

    private QueryStatus modify(String metric, MetricStatus status) {
        if (containsExpressions(metric)) {
            return QueryStatus.WRONG;
        }
        boolean isDir = metric.charAt(metric.length() - 1) == '.';

        String[] levels = metric.split("\\.");
        Dir dir = root;
        for (int i = 0; i < levels.length; i++) {
            boolean isLast = (i == levels.length - 1);
            if (!isLast && dir.isBan()) {
                return QueryStatus.BAN;
            }
            String level = levels[i];
            if (!isLast) {
                dir = dir.getOrCreateDir(level);
                checkParentDirVisibility(dir, status);
            } else {
                if (status.equals(MetricStatus.SIMPLE)) {
                    return dir.createMetric(level);
                } else {
                    return modify(dir, level, isDir, status);
                }
            }
        }
        throw new IllegalStateException();
    }

    private QueryStatus modify(Dir parent, String name, boolean isDir, MetricStatus status) {
        if (isDir) {
            Dir dir = parent.getOrCreateDir(name);
            dir.status = selectStatus(dir.status, status);
        } else {
            MetricName metric = parent.getOrCreateMetric(name);
            metric.status = selectStatus(metric.status, status);
        }
        return QueryStatus.UPDATED;
    }

    private void checkParentDirVisibility(Dir dir, MetricStatus status) {
        if (!status.visible()) {
            return;
        }
        if (dir.status.equals(MetricStatus.HIDDEN) || dir.status.equals(MetricStatus.AUTO_HIDDEN)) {
            dir.status = MetricStatus.SIMPLE;
        }
    }

    private MetricStatus selectStatus(MetricStatus oldStatus, MetricStatus newStatus) {
        if (oldStatus.equals(newStatus)) {
            return newStatus;
        }
        if (newStatus.handmade() || !oldStatus.handmade()) {
            return newStatus;
        }
        if (oldStatus.equals(MetricStatus.HIDDEN) && newStatus.equals(MetricStatus.SIMPLE)) {
            return newStatus;
        }
        return oldStatus;
    }

    private boolean containsExpressions(String metric) {
        return metric.contains("*") || metric.contains("?");
    }

    private static class Dir {
        private final Dir parent;
        private final String name;
        private final ConcurrentMap<String, MetricName> metrics = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Dir> dirs = new ConcurrentHashMap<>();
        private volatile MetricStatus status = MetricStatus.SIMPLE;

        public Dir(Dir parent, String name) {
            this.parent = parent;
            this.name = name.intern();
        }

        private Dir getOrCreateDir(String name) {
            Dir dir = dirs.get(name);
            if (dir != null) {
                return dir;
            }
            Dir newDir = new Dir(this, name);
            dir = dirs.putIfAbsent(name, newDir);
            return dir == null ? newDir : dir;
        }

        private QueryStatus createMetric(String name) {
            if (metrics.containsKey(name)) {
                return QueryStatus.UPDATED;
            }
            MetricName newMetricName = new MetricName(this, name);
            if (metrics.putIfAbsent(name, newMetricName) == null) {
                return QueryStatus.NEW;
            } else {
                return QueryStatus.UPDATED;
            }
        }

        private MetricName getOrCreateMetric(String name) {
            MetricName metricName = metrics.get(name);
            if (metricName != null) {
                return metricName;
            }
            createMetric(name);
            return metrics.get(name);
        }

        private boolean isRoot() {
            return parent == null;
        }

        private boolean isBan() {
            return status.equals(MetricStatus.BAN);
        }

        @Override
        public String toString() {
            if (isRoot()) {
                return "ROOT";
            }
            if (parent.isRoot()) {
                return name;
            } else {
                return parent.toString() + "." + name;
            }
        }
    }

    private static class MetricName {
        private final Dir parent;
        private final String name;

        public MetricName(Dir parent, String name) {
            this.parent = parent;
            this.name = name.intern();
        }

        private volatile MetricStatus status = MetricStatus.SIMPLE;

        @Override
        public String toString() {
            return parent.toString() + "." + name;
        }
    }


}
