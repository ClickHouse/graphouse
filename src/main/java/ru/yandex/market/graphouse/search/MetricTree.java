package ru.yandex.market.graphouse.search;

import com.google.common.base.CharMatcher;
import ru.yandex.market.graphouse.utils.AppendableResult;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.PatternSyntaxException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 07/04/15
 */
public class MetricTree {

    public static final String ALL_PATTERN = "*";
    public static final String LEVEL_SPLITTER = ".";
    public static final String LEVEL_SPLIT_PATTERN = "\\.";

    private static final CharMatcher EXPRESSION_MATCHER = CharMatcher.anyOf(ALL_PATTERN + "?[]{}");
    private final Dir root = new Dir(null, "", MetricStatus.SIMPLE);

    public void search(String query, AppendableResult result) throws IOException {
        String[] levels = query.split(LEVEL_SPLIT_PATTERN);
        search(root, levels, 0, result);
    }

    /**
     * Рекурсивный метод для получения списка метрик внутри дерева.
     *
     * @param dir        внутри какой директории ищем
     * @param levels     узлы дерева, каждый может быть задан явно или паттерном, используя *?[]{}
     *                   Пример: five_min.abo-main.timings-method.*.0_95
     * @param levelIndex индекс текущего узла
     * @param result
     * @throws IOException
     */
    private void search(Dir dir, String[] levels, int levelIndex, AppendableResult result) throws IOException {
        if (dir == null || !dir.visible()) {
            return;
        }
        boolean isLast = (levelIndex == levels.length - 1);
        String level = levels[levelIndex];
        boolean isPattern = containsExpressions(level);

        if (isLast) {
            if (!isPattern) {
                appendSimpleResult(dir.metrics, level, result);
                appendSimpleResult(dir.dirs, level, result);
            } else if (level.equals(ALL_PATTERN)) {
                appendAllResult(dir.metrics, result);
                appendAllResult(dir.dirs, result);
            } else {
                appendAllPatternResult(dir.metrics, level, result);
                appendAllPatternResult(dir.dirs, level, result);
            }
        } else {
            if (dir.hasDirs()) {
                for (Dir subDir : dir.dirs.values()) {
                    boolean matches = (!isPattern && subDir.name.equals(level))
                        || level.equals(ALL_PATTERN)
                        || matches(createPathMatcher(level), subDir.name);

                    if (matches) {
                        search(subDir, levels, levelIndex + 1, result);
                    }
                }
            }
        }
    }

    private <T extends MetricBase> void appendAllPatternResult(Map<String, T> map, String pattern, AppendableResult result) throws IOException {
        if (map != null) {
            final PathMatcher pathMatcher = createPathMatcher(pattern);
            for (MetricBase metricBase : map.values()) {
                if (matches(pathMatcher, metricBase.name)) {
                    appendResult(metricBase, result);
                }
            }
        }
    }

    private <T extends MetricBase> void appendAllResult(Map<String, T> map, AppendableResult result) throws IOException {
        if (map != null) {
            for (MetricBase metricBase : map.values()) {
                appendResult(metricBase, result);
            }
        }
    }

    private <T extends MetricBase> void appendSimpleResult(Map<String, T> map, String name, AppendableResult result) throws IOException {
        if (map != null) {
            appendResult(map.get(name), result);
        }
    }

    private static void appendResult(MetricBase metricBase, AppendableResult result) throws IOException {
        if (metricBase != null && metricBase.visible()) {
            result.appendMetric(metricBase);
        }
    }

    static PathMatcher createPathMatcher(String globPattern) {
        try {
            return FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
        } catch (PatternSyntaxException e) {
            return null;
        }
    }

    static boolean matches(PathMatcher pathMatcher, final String fileName) {
        Path mockPath = new MetricPath(fileName);
        return pathMatcher.matches(mockPath);
    }

    int metricCount() {
        return root.metricCount();
    }

    int dirCount() {
        return root.dirCount();
    }

    MetricDescription add(String metric) {
        return modify(metric, MetricStatus.SIMPLE);
    }

    /**
     * Создает или изменяет статус метрики или целой директории.
     *
     * @param metric если заканчивается на {@link MetricTree#LEVEL_SPLITTER} , то директория
     * @param status
     * @return null если метрика/директория забанена. Иначе MetricDescription
     */
    MetricDescription modify(String metric, MetricStatus status) {
        boolean isDir = metric.endsWith(LEVEL_SPLITTER);

        String[] levels = metric.split(LEVEL_SPLIT_PATTERN);
        if (levels.length == 1) {
            throw new IllegalArgumentException("Disallowed modify second level");
        }

        Dir dir = root;
        for (int i = 0; i < levels.length; i++) {
            boolean isLast = (i == levels.length - 1);
            if (dir.getStatus() == MetricStatus.BAN) {
                return null;
            }
            String level = levels[i];
            if (!isLast) {
                dir = dir.getOrCreateDirWithStatus(level, status);
            } else {
                MetricBase metricBase = isDir ? dir.getOrCreateDirWithStatus(level, status) : dir.getOrCreateMetricWithStatus(level, status);
                metricBase.setStatus(selectStatus(metricBase.getStatus(), status));
                if (status.visible() != dir.visible()) {
                    updatePathVisibility(dir);
                }
                return metricBase;
            }
        }
        throw new IllegalStateException();
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
            dir.getStatus(),
            hasVisibleChildren(dir) ? MetricStatus.SIMPLE : MetricStatus.AUTO_HIDDEN
        );
        if (dir.getStatus() != newStatus) {
            dir.setStatus(newStatus);
            updatePathVisibility(dir.parent);
        }
    }

    private boolean hasVisibleChildren(Dir dir) {
        if (dir.hiddenElements != 0) {
            int count = 0;
            if (dir.hasDirs()) {
                count += dir.dirs.size();
            }
            if (dir.hasMetrics()) {
                count += dir.metrics.size();
            }
            return dir.hiddenElements != count;
        }
        return true;
    }

    /**
     * Возвращаем новый статус при изменении метрики, учитывая граф возможных переходов.
     *
     * @param oldStatus
     * @param newStatus
     * @return
     */
    private MetricStatus selectStatus(MetricStatus oldStatus, MetricStatus newStatus) {
        if (oldStatus == newStatus) {
            return oldStatus;
        }

        List<MetricStatus> restricted = MetricStatus.RESTRICTED_GRAPH_EDGES.get(oldStatus);
        return restricted == null || !restricted.contains(newStatus) ? newStatus : oldStatus;
    }

    static boolean containsExpressions(String metric) {
        return EXPRESSION_MATCHER.matchesAnyOf(metric);
    }

    private abstract static class MetricBase implements MetricDescription {
        final Dir parent;
        final String name;

        private volatile long updateTimeMillis = System.currentTimeMillis();
        private volatile MetricStatus status = MetricStatus.SIMPLE;

        MetricBase(Dir parent, String name, MetricStatus status) {
            this.parent = parent;
            this.name = name;
            this.status = status;

            if (!status.visible() && !isRoot()) {
                parent.incrementHiddenCounter();
            }
        }

        boolean visible() {
            return status.visible();
        }

        boolean isRoot() {
            return parent == null;
        }

        @Override
        public MetricStatus getStatus() {
            return status;
        }

        void setStatus(MetricStatus status) {
            if (this.status != status) {
                if (this.status.visible() != status.visible() && !isRoot()) {
                    if (status.visible()) {
                        this.parent.incrementHiddenCounter();
                    } else {
                        this.parent.decrementHiddenCounter();
                    }
                }
                this.status = status;
                updateTimeMillis = System.currentTimeMillis();
            }
        }

        @Override
        public String toString() {
            return getName();
        }

        @Override
        public long getUpdateTimeMillis() {
            return updateTimeMillis;
        }
    }

    private static class Dir extends MetricBase {
        private volatile Map<String, MetricName> metrics;
        private volatile Map<String, Dir> dirs;
        private volatile int hiddenElements = 0;

        public Dir(Dir parent, String name, MetricStatus status) {
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

        boolean hasDirs() {
            return dirs != null && !dirs.isEmpty();
        }

        boolean hasMetrics() {
            return metrics != null && !metrics.isEmpty();
        }

        private Dir getOrCreateDirWithStatus(String name, MetricStatus status) {
            initDirs();
            return dirs.computeIfAbsent(name, d -> new Dir(this, name.intern(), status));
        }


        private MetricName getOrCreateMetricWithStatus(String name, MetricStatus status) {
            initMetrics();
            return metrics.computeIfAbsent(name, m -> new MetricName(this, name.intern(), status));
        }

        int metricCount() {
            int count = 0;

            if (hasMetrics()) {
                count = metrics.size();
            }

            if (hasDirs()) {
                for (Dir dir : dirs.values()) {
                    count += dir.metricCount();
                }
            }
            return count;
        }

        int dirCount() {
            if (!hasDirs()) {
                return 0;
            }

            int count = dirs.size();
            for (Dir dir : dirs.values()) {
                count += dir.dirCount();
            }
            return count;
        }

        @Override
        public boolean isDir() {
            return true;
        }

        @Override
        public String getName() {
            if (isRoot()) {
                return "ROOT";
            }

            final String dirName = name + LEVEL_SPLITTER;
            return parent.isRoot() ? dirName : parent.toString() + dirName;
        }

        synchronized void incrementHiddenCounter() {
            hiddenElements++;
        }

        synchronized void decrementHiddenCounter() {
            hiddenElements--;
        }
    }

    private static class MetricName extends MetricBase {

        MetricName(Dir parent, String name, MetricStatus status) {
            super(parent, name, status);
        }

        @Override
        public boolean isDir() {
            return false;
        }

        @Override
        public String getName() {
            return parent.isRoot() ? name : parent.toString() + name;
        }
    }
}
