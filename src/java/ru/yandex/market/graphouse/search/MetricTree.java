package ru.yandex.market.graphouse.search;

import com.google.common.base.CharMatcher;
import org.apache.http.util.ByteArrayBuffer;

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
        if (!parentDir.getStatus().visible()) {
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

    public int metricCount() {
        return root.metricCount();
    }

    public int dirCount() {
        return root.dirCount();
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
            if (dir.visible() && pattern.matcher(dirEntry.getKey()).matches()) {
                appendResult(dir, result);
            }
        }
        for (Map.Entry<String, MetricName> metricEntry : parentDir.metrics.entrySet()) {
            MetricName metricName = metricEntry.getValue();
            if (metricName.visible() && pattern.matcher(metricEntry.getKey()).matches()) {
                appendResult(metricName, result);
            }
        }
    }

    private void appendResult(Dir dir, Appendable result) throws IOException {
        if (dir != null && dir.visible()) {
            appendDir(dir, result);
            result.append('\n');
        }
    }

    private void appendResult(MetricName metric, Appendable result) throws IOException {
        if (metric != null && metric.visible()) {
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

    public MetricDescription add(String metric) {
        return modify(metric, MetricStatus.SIMPLE);
    }

    /**
     * Создает или изменяет статус метрики или целой директории.
     *
     * @param metric если заканчивается на '.' , то директория
     * @param status
     * @return null если метрика/директория забанена. Иначе MetricDescription
     */
    public MetricDescription modify(String metric, MetricStatus status) {
        boolean isDir = metric.charAt(metric.length() - 1) == '.';

        String[] levels = metric.split("\\.");
        Dir dir = root;
        for (int i = 0; i < levels.length; i++) {
            boolean isLast = (i == levels.length - 1);
            if (dir.getStatus() == MetricStatus.BAN) {
                return null;
            }
            String level = levels[i];
            if (!isLast) {
                dir = dir.getOrCreateDir(level);
                if (dir.parent.visible() != dir.visible()) {
                    updatePathVisibility(dir.parent);
                }
            } else {
                MetricDescription metricDescription = modify(dir, level, isDir, status);
                if (status.visible() != dir.visible()) {
                    updatePathVisibility(dir);
                }
                return metricDescription;
            }
        }
        throw new IllegalStateException();
    }

    private MetricDescription modify(Dir parent, String name, boolean isDir, MetricStatus status) {
        if (parent.isRoot()) {
            throw new IllegalArgumentException("Disallowed modify second level");
        }
        if (isDir) {
            Dir dir = parent.getOrCreateDir(name);
            dir.setStatus(selectStatus(dir.getStatus(), status));
            return dir;
        } else {
            MetricName metric = parent.getOrCreateMetric(name);
            metric.setStatus(selectStatus(metric.getStatus(), status));
            return metric;
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
            dir.getStatus(),
            hasVisibleChildren(dir) ? MetricStatus.SIMPLE : MetricStatus.AUTO_HIDDEN
        );
        if (dir.getStatus() != newStatus) {
            dir.setStatus(newStatus);
            updatePathVisibility(dir.parent);
        }
    }

    private boolean hasVisibleChildren(Dir dir) {
        for (Dir child : dir.dirs.values()) {
            if (child.visible()) {
                return true;
            }
        }
        for (MetricName child : dir.metrics.values()) {
            if (child.visible()) {
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

    private static class Dir extends MetricBase {
        private final ConcurrentMap<String, MetricName> metrics = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Dir> dirs = new ConcurrentHashMap<>();

        public Dir(Dir parent, String name) {
            super(parent, name);
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


        private MetricName getOrCreateMetric(String name) {
            MetricName metricName = metrics.get(name);
            if (metricName != null) {
                return metricName;
            }
            MetricName newMetricName = new MetricName(this, name);
            metricName = metrics.putIfAbsent(name, newMetricName);
            return metricName == null ? newMetricName : metricName;
        }

        private MetricName get(String metric) {
            return metrics.get(metric);
        }

        public int metricCount() {
            int count = metrics.size();
            for (Dir dir : dirs.values()) {
                count += dir.metricCount();
            }
            return count;
        }

        public int dirCount() {
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
    }

    private static class MetricName extends MetricBase {

        public MetricName(Dir parent, String name) {
            super(parent, name);
        }

        @Override
        public boolean isDir() {
            return false;
        }
    }

    private abstract static class MetricBase implements MetricDescription {
        protected final Dir parent;
        protected final String name;

        private volatile long updateTimeMillis = System.currentTimeMillis();
        private volatile MetricStatus status = MetricStatus.SIMPLE;

        public MetricBase(Dir parent, String name) {
            this.parent = parent;
            this.name = name.intern();
        }

        public boolean visible() {
            return status.visible();
        }

        public boolean isRoot() {
            return parent == null;
        }

        @Override
        public void writeName(ByteArrayBuffer buffer) {
            if (isRoot()) {
                appendBytes(buffer, "ROOT".getBytes());
                return;
            }
            if (!parent.isRoot()) {
                parent.writeName(buffer);
            }
            buffer.append('.');
            appendBytes(buffer, name.getBytes());
        }

        @Override
        public String getName() {
            if (isRoot()) {
                return "ROOT";
            }
            return parent.toString() + "." + name;
        }

        @Override
        public MetricStatus getStatus() {
            return status;
        }

        public void setStatus(MetricStatus status) {
            if (this.status != status) {
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

    private static void appendBytes(ByteArrayBuffer buffer, byte[] bytes) {
        buffer.append(bytes, 0, bytes.length);
    }

}
