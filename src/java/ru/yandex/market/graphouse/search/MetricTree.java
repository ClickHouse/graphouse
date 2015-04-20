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
        if (parentDir.ban) {
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
        globPattern = globPattern.replace("*", "[-_0-9a-zA-Z]+");
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
            if (pattern.matcher(dirEntry.getKey()).matches()) {
                appendResult(dirEntry.getValue(), result);
            }
        }
        for (Map.Entry<String, MetricName> metricEntry : parentDir.metrics.entrySet()) {
            if (pattern.matcher(metricEntry.getKey()).matches()) {
                appendResult(metricEntry.getValue(), result);
            }
        }
    }

    private void appendResult(Dir dir, Appendable result) throws IOException {
        if (dir != null && !dir.ban) {
            appendDir(dir, result);
            result.append('\n');
        }
    }

    private void appendResult(MetricName metric, Appendable result) throws IOException {
        if (metric != null && !metric.ban) {
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

    /**
     * @param metric
     * @return true - если новая метрика была добавлена
     */
    public MetricStatus add(String metric) {
        return modify(metric, false);
    }


    public void ban(String metric) {
        modify(metric, true);
    }

    private MetricStatus modify(String metric, boolean ban) {
        if (containsExpressions(metric)) {
            return MetricStatus.WRONG;
        }
        boolean isDir = metric.charAt(metric.length() - 1) == '.';
        if (isDir && !ban) {
            return MetricStatus.WRONG;
        }

        String[] levels = metric.split("\\.");
        Dir dir = root;
        for (int i = 0; i < levels.length; i++) {
            if (dir.ban) {
                return MetricStatus.BAN;
            }
            String level = levels[i];
            boolean isLast = (i == levels.length - 1);
            if (!isLast) {
                dir = dir.getOrCreateDir(level);
            } else {
                if (ban) {
                    ban(dir, level, isDir);
                    return MetricStatus.BAN;
                } else {
                    return dir.createMetric(level);
                }
            }
        }
        throw new IllegalStateException();
    }

    private void ban(Dir parent, String name, boolean isDir) {
        if (isDir) {
            parent.getOrCreateDir(name).ban = true;
        } else {
            parent.getOrCreateMetric(name).ban = true;
        }
    }

    private boolean containsExpressions(String metric) {
        return metric.contains("*") || metric.contains("?");
    }

    private static class Dir {
        private final Dir parent;
        private final String name;
        private final ConcurrentMap<String, MetricName> metrics = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Dir> dirs = new ConcurrentHashMap<>();
        private volatile boolean ban = false;

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

        private MetricStatus createMetric(String name) {
            if (metrics.containsKey(name)) {
                return MetricStatus.EXISTING;
            }
            MetricName newMetricName = new MetricName(this, name);
            if (metrics.putIfAbsent(name, newMetricName) == null) {
                return MetricStatus.NEW;
            } else {
                return MetricStatus.EXISTING;
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

        private volatile boolean ban = false;

        @Override
        public String toString() {
            return parent.toString() + "." + name;
        }
    }


}
