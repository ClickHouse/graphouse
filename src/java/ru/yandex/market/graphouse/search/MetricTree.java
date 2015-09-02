package ru.yandex.market.graphouse.search;

import com.google.common.base.CharMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 07/04/15
 */
public class MetricTree {

    private static final CharMatcher EXPRESSION_MATCHER = CharMatcher.anyOf("*?[]{}");
    private final Dir root = new Dir(null, "");

    public void search(String query, Appendable result) throws IOException {
        String[] levels = query.split("\\.");
        search(root, levels, 0, result);
    }

    /**
     * Рекурсивный метод для получения списка метрик внутри дерева.
     *
     * @param parentDir  внутри какой директории ищем
     * @param levels     узлы дерева, каждый может быть задан явно или паттерном, используя *?[]{}
     *                   Пример: five_min.abo-main.timings-method.*.0_95
     * @param levelIndex индекс текущего узла
     * @param result
     * @throws IOException
     */
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
            if (pattern == null) {
                return;
            }
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

    public static Pattern createPattern(String globPattern) {
        globPattern = globPattern.replace("*", "[-_0-9a-zA-Z]*");
        globPattern = globPattern.replace("?", "[-_0-9a-zA-Z]");
        try {
            return Pattern.compile(globPattern);
        } catch (PatternSyntaxException e) {
            return null;
        }
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

    /**
     * Создает или изменяет статус метрики или целой директории.
     *
     * @param metric если заканчивается на '.' , то директория
     * @param status
     * @return
     */
    private QueryStatus modify(String metric, MetricStatus status) {
        if (containsExpressions(metric)) {
            return QueryStatus.WRONG;
        }
        boolean isDir = metric.charAt(metric.length() - 1) == '.';

        String[] levels = metric.split("\\.");
        Dir dir = root;
        for (int i = 0; i < levels.length; i++) {
            boolean isLast = (i == levels.length - 1);
            if (dir.isBan()) {
                return QueryStatus.BAN;
            }
            String level = levels[i];
            if (!isLast) {
                dir = dir.getOrCreateDir(level);
                if (dir.parent.status.visible() != dir.status.visible()) {
                    updatePathVisibility(dir.parent);
                }
            } else {
                QueryStatus queryStatus = modify(dir, level, isDir, status);
                if (status.visible() != dir.status.visible()) {
                    updatePathVisibility(dir);
                }
                return queryStatus;
            }
        }
        throw new IllegalStateException();
    }

    private QueryStatus modify(Dir parent, String name, boolean isDir, MetricStatus status) {
        if (parent.isRoot()) {
            return QueryStatus.WRONG; // Не даем править второй уровень.
        }
        if (isDir) {
            Dir dir = parent.getOrCreateDir(name);
            MetricStatus oldStatus = dir.status;
            dir.status = selectStatus(oldStatus, status);
            return (oldStatus == dir.status) ? QueryStatus.UNMODIFIED : QueryStatus.UPDATED;
        } else {
            QueryStatus queryStatus = parent.createMetric(name);
            MetricName metric = parent.get(name);
            metric.status = selectStatus(metric.status, status);
            return queryStatus;
        }
    }

    /**
     * Если все метрики в директории скрыты, то пытаемся скрыть её {@link MetricStatus#AUTO_HIDDEN}
     * Если для директории есть хоть одна открытая метрика, то пытаемся открыть её {@link MetricStatus#SIMPLE}
     *
     * @param dir
     */
    private void updatePathVisibility(Dir dir) {
        if (dir.isRoot()) {
            return;
        }
        MetricStatus newStatus = selectStatus(
            dir.status,
            hasVisibleChildren(dir) ? MetricStatus.SIMPLE : MetricStatus.AUTO_HIDDEN
        );
        if (dir.status != newStatus) {
            dir.status = newStatus;
            updatePathVisibility(dir.parent);
        }
    }

    private boolean hasVisibleChildren(Dir dir) {
        for (Dir child : dir.dirs.values()) {
            if (child.status.visible()) {
                return true;
            }
        }
        for (MetricName child : dir.metrics.values()) {
            if (child.status.visible()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Возвращаем новый статус при изменении метрики, учитывая граф возможных переходов.
     *
     * @param oldStatus
     * @param newStatus
     * @return
     */
    private MetricStatus selectStatus(MetricStatus oldStatus, MetricStatus newStatus) {
        List<MetricStatus> restricted = MetricStatus.RESTRICTED_GRAPH_EDGES.get(oldStatus);
        return restricted == null || !restricted.contains(newStatus) ? newStatus : oldStatus;
    }

    public static boolean containsExpressions(String metric) {
        return EXPRESSION_MATCHER.matchesAnyOf(metric);
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

        private QueryStatus createMetric(String metric) {
            if (metrics.containsKey(metric)) {
                return QueryStatus.UPDATED;
            }
            MetricName newMetricName = new MetricName(this, metric);
            if (metrics.putIfAbsent(metric, newMetricName) == null) {
                return QueryStatus.NEW;
            } else {
                return QueryStatus.UPDATED;
            }
        }

        private QueryStatus createIfNotExists(String metric) {
            MetricName metricName = metrics.get(metric);
            if (metricName != null) {
                return QueryStatus.UPDATED;
            }
            createMetric(metric);
            return QueryStatus.NEW;
        }

        private MetricName get(String metric) {
            return metrics.get(metric);
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

        private volatile MetricStatus status = MetricStatus.SIMPLE;

        public MetricName(Dir parent, String name) {
            this.parent = parent;
            this.name = name.intern();
        }

        @Override
        public String toString() {
            return parent.toString() + "." + name;
        }
    }
}
