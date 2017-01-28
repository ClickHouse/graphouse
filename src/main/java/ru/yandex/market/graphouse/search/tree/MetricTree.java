package ru.yandex.market.graphouse.search.tree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.search.MetricPath;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.utils.AppendableResult;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 07/04/15
 */
public class MetricTree {

    public static final String ALL_PATTERN = "*";

    private static final CharMatcher EXPRESSION_MATCHER = CharMatcher.anyOf(ALL_PATTERN + "?[]{}");
    private final MetricDir root = new InMemoryMetricDir(null, "", MetricStatus.SIMPLE);

    private final MetricDirFactory metricDirFactory;

    public MetricTree(MetricDirFactory metricDirFactory) {
        this.metricDirFactory = metricDirFactory;
    }

    public void search(String query, AppendableResult result) throws IOException {
        String[] levels = MetricUtil.splitToLevels(query);
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
    private void search(MetricDir parentDir, String[] levels, int levelIndex, AppendableResult result) throws IOException {
        if (parentDir == null || !parentDir.visible()) {
            return;
        }
        boolean isLast = (levelIndex == levels.length - 1);
        String level = levels[levelIndex];
        boolean isPattern = containsExpressions(level);


        if (!isPattern) {
            if (parentDir.hasDirs()) {
                if (isLast) {
                    appendSimpleResult(parentDir.getDirs(), level, result);
                } else {
                    search(parentDir.getDirs().get(level), levels, levelIndex + 1, result);
                }
            }
            if (isLast && parentDir.hasMetrics()) {
                appendSimpleResult(parentDir.getMetrics(), level, result);
            }
        } else if (level.equals(ALL_PATTERN)) {
            if (parentDir.hasDirs()) {
                if (isLast) {
                    appendAllResult(parentDir.getDirs(), result);
                } else {
                    for (MetricDir dir : parentDir.getDirs().values()) {
                        search(dir, levels, levelIndex + 1, result);
                    }
                }
            }
            if (isLast && parentDir.hasMetrics()) {
                appendAllResult(parentDir.getMetrics(), result);
            }
        } else {
            PathMatcher pathMatcher = createPathMatcher(level);
            if (pathMatcher == null) {
                return;
            }
            if (parentDir.hasDirs()) {
                if (isLast) {
                    appendAllPatternResult(parentDir.getDirs(), pathMatcher, result);
                } else {
                    for (Map.Entry<String, MetricDir> dirEntry : parentDir.getDirs().entrySet()) {
                        if (matches(pathMatcher, dirEntry.getKey())) {
                            search(dirEntry.getValue(), levels, levelIndex + 1, result);
                        }
                    }
                }
            }
            if (isLast && parentDir.hasMetrics()) {
                appendAllPatternResult(parentDir.getMetrics(), pathMatcher, result);
            }
        }

    }

    private <T extends MetricBase> void appendAllPatternResult(Map<String, T> map, PathMatcher pathMatcher,
                                                               AppendableResult result) throws IOException {
        if (map != null) {
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

    @VisibleForTesting
    static PathMatcher createPathMatcher(String globPattern) {
        try {
            return FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
        } catch (PatternSyntaxException e) {
            return null;
        }
    }

    @VisibleForTesting
    static boolean matches(PathMatcher pathMatcher, final String fileName) {
        Path mockPath = new MetricPath(fileName);
        return pathMatcher.matches(mockPath);
    }

    public int metricCount() {
        return root.loadedMetricCount();
    }

    public int dirCount() {
        return root.loadedDirCount();
    }

    public MetricDescription add(String metric) {
        return modify(metric, MetricStatus.SIMPLE);
    }


    /**
     * Создает или изменяет статус метрики или целой директории.
     *
     * @param metric если заканчивается на ., то директория
     * @param status
     * @return null если метрика/директория забанена. Иначе MetricDescription
     */
    public MetricDescription modify(String metric, MetricStatus status) {
        boolean isDir = MetricUtil.isDir(metric);

        String[] levels = MetricUtil.splitToLevels(metric);
        if (levels.length == 1) {
            throw new IllegalArgumentException("Disallowed modify second level");
        }

        MetricDir dir = root;
        for (int i = 0; i < levels.length; i++) {
            boolean isLast = (i == levels.length - 1);
            if (dir.getStatus() == MetricStatus.BAN) {
                return null;
            }
            String level = levels[i];
            if (!isLast) {
                dir = dir.getOrCreateDir(level, status, metricDirFactory);
            } else {
                MetricBase metricBase;
                if (isDir) {
                    metricBase = dir.getOrCreateDir(level, status, metricDirFactory);
                } else {
                    metricBase = dir.getOrCreateMetric(level, status);
                }
                metricBase.setStatus(selectStatus(metricBase.getStatus(), status));
                return metricBase;
            }
        }
        throw new IllegalStateException();
    }

    /**
     * Возвращаем новый статус при изменении метрики, учитывая граф возможных переходов.
     *
     * @param oldStatus
     * @param newStatus
     * @return
     */
    public static MetricStatus selectStatus(MetricStatus oldStatus, MetricStatus newStatus) {
        if (oldStatus == newStatus) {
            return oldStatus;
        }

        List<MetricStatus> restricted = MetricStatus.RESTRICTED_GRAPH_EDGES.get(oldStatus);
        return restricted == null || !restricted.contains(newStatus) ? newStatus : oldStatus;
    }

    @VisibleForTesting
    static boolean containsExpressions(String metric) {
        return EXPRESSION_MATCHER.matchesAnyOf(metric);
    }


}
