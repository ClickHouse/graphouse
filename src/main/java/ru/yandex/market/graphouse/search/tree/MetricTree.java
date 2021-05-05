package ru.yandex.market.graphouse.search.tree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.retention.RetentionProvider;
import ru.yandex.market.graphouse.search.MetricPath;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.utils.AppendableResult;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 07/04/15
 */
public class MetricTree {

    public static final String ALL_PATTERN = "*";

    private static final CharMatcher EXPRESSION_MATCHER = CharMatcher.anyOf(ALL_PATTERN + "?[]{}");
    final MetricDir root = new InMemoryMetricDir(null, "", MetricStatus.SIMPLE);

    private final MetricDirFactory metricDirFactory;
    private final RetentionProvider retentionProvider;
    private final int maxSubDirsPerDir;
    private final int maxMetricsPerDir;

    private final String subdirsPerDirLimitMessage;
    private final String metricsPerDirLimitMessage;


    public MetricTree(MetricDirFactory metricDirFactory, RetentionProvider retentionProvider,
                      int maxSubDirsPerDir, int maxMetricsPerDir) {
        this.metricDirFactory = metricDirFactory;
        this.retentionProvider = retentionProvider;
        this.maxSubDirsPerDir = maxSubDirsPerDir;
        subdirsPerDirLimitMessage = "_SUBDIRS_LIMIT_REACHED_MAX_" + maxSubDirsPerDir;
        this.maxMetricsPerDir = maxMetricsPerDir;
        metricsPerDirLimitMessage = "_METRICS_LIMIT_REACHED_MAX_" + maxMetricsPerDir;
    }

    public void search(String query, AppendableResult result) throws IOException {
        String[] levels = MetricUtil.splitToLevels(query);
        search(root, levels, 0, result);
    }

    public void searchCachedMetrics(String query, AppendableResult result) throws IOException {
        String[] levels = MetricUtil.splitToLevels(query);
        searchCachedMetrics(root, levels, 0, result);
    }

    /**
     * Recursive method for getting a list of metrics inside the tree
     *
     * @param parentDir  the directory we are searching in
     * @param levels     tree nodes, each can be set explicitly or by a pattern using *?[]{}
     *                   Example: five_min.abo-main.timings-method.*.0_95
     * @param levelIndex index of the current node
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
                    appendSimpleResult(parentDir.getDirs(), level, result, false);
                } else {
                    search(parentDir.getDirs().get(level), levels, levelIndex + 1, result);
                }
            }
            if (isLast && parentDir.hasMetrics()) {
                appendSimpleResult(parentDir.getMetrics(), level, result, false);
            }
        } else if (level.equals(ALL_PATTERN)) {
            if (isLast) {
                appendLimitNotificationsIfNeeded(parentDir, result);
                if (parentDir.hasDirs()) {
                    appendAllResult(parentDir.getDirs(), result, false);
                }
                if (parentDir.hasMetrics()) {
                    appendAllResult(parentDir.getMetrics(), result, false);
                }
            } else {
                for (MetricDir dir : parentDir.getDirs().values()) {
                    search(dir, levels, levelIndex + 1, result);
                }
            }
        } else {
            PathMatcher pathMatcher = createPathMatcher(level);
            if (pathMatcher == null) {
                return;
            }
            if (parentDir.hasDirs()) {
                if (isLast) {
                    appendAllPatternResult(parentDir.getDirs(), pathMatcher, result, false);
                } else {
                    for (Map.Entry<String, MetricDir> dirEntry : parentDir.getDirs().entrySet()) {
                        if (matches(pathMatcher, dirEntry.getKey())) {
                            search(dirEntry.getValue(), levels, levelIndex + 1, result);
                        }
                    }
                }
            }
            if (isLast && parentDir.hasMetrics()) {
                appendAllPatternResult(parentDir.getMetrics(), pathMatcher, result, false);
            }
        }
    }


    /**
     * Extract cached metric from the tree
     *
     * @param parentDir  the directory we are searching in
     * @param levels     tree nodes, each can be set explicitly or by a pattern using *?[]{}
     *                   Example: five_min.abo-main.timings-method.*.0_95
     * @param levelIndex index of the current node
     */
    private void searchCachedMetrics(MetricDir parentDir, String[] levels, int levelIndex, AppendableResult result) throws IOException {
        if (parentDir == null) {
            return;
        }
        result.appendMetric(parentDir);

        boolean isLast = (levelIndex == levels.length - 1);
        String level = levels[levelIndex];
        boolean isPattern = containsExpressions(level);

        Map<String, MetricDir> dirs = parentDir.maybeGetDirs();
        Map<String, MetricName> metrics = parentDir.maybeGetMetrics();
        if (!isPattern) {
            if (isLast) {
                appendSimpleResult(dirs, level, result, true);
            } else {
                searchCachedMetrics(dirs.get(level), levels, levelIndex + 1, result);
            }
            if (isLast) {
                appendSimpleResult(metrics, level, result, true);
            }
        } else if (level.equals(ALL_PATTERN)) {
            if (isLast) {
                appendAllResult(dirs, result, true);
                appendAllResult(metrics, result, true);
            } else {
                for (MetricDir dir : dirs.values()) {
                    searchCachedMetrics(dir, levels, levelIndex + 1, result);
                }
            }
        } else {
            PathMatcher pathMatcher = createPathMatcher(level);
            if (pathMatcher == null) {
                return;
            }
            if (isLast) {
                appendAllPatternResult(dirs, pathMatcher, result, true);
            } else {
                for (Map.Entry<String, MetricDir> dirEntry : dirs.entrySet()) {
                    if (matches(pathMatcher, dirEntry.getKey())) {
                        searchCachedMetrics(dirEntry.getValue(), levels, levelIndex + 1, result);
                    }
                }
            }
            if (isLast) {
                appendAllPatternResult(metrics, pathMatcher, result, true);
            }
        }
    }

    private void appendLimitNotificationsIfNeeded(MetricDir parentDir, AppendableResult result) throws IOException {
        if (maxSubDirsPerDir > 0 && parentDir.hasDirs() && parentDir.getDirs().size() >= maxSubDirsPerDir) {
            result.appendMetric(new NotificationMetric(parentDir, subdirsPerDirLimitMessage));
        }
        if (maxMetricsPerDir > 0 && parentDir.hasMetrics() && parentDir.getMetrics().size() >= maxMetricsPerDir) {
            result.appendMetric(new NotificationMetric(parentDir, metricsPerDirLimitMessage));
        }
    }

    private <T extends MetricBase> void appendAllPatternResult(
        Map<String, T> map, PathMatcher pathMatcher, AppendableResult result, boolean forceAppend
    ) throws IOException {
        if (map != null) {
            for (MetricBase metricBase : map.values()) {
                if (matches(pathMatcher, metricBase.name)) {
                    appendResult(metricBase, result, forceAppend);
                }
            }
        }
    }

    private <T extends MetricBase> void appendAllResult(
        Map<String, T> map, AppendableResult result, boolean forceAppend
    ) throws IOException {
        if (map != null) {
            for (MetricBase metricBase : map.values()) {
                appendResult(metricBase, result, forceAppend);
            }
        }
    }

    private <T extends MetricBase> void appendSimpleResult(
        Map<String, T> map, String name, AppendableResult result, boolean forceAppend
    ) throws IOException {
        if (map != null) {
            appendResult(map.get(name), result, forceAppend);
        }
    }

    private static void appendResult(MetricBase metricBase, AppendableResult result, boolean forceAppend) throws IOException {
        if (metricBase != null && (forceAppend || metricBase.visible())) {
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
     * Try to find metric description. Fast. Can return null, even if metric exists
     *
     * @param levels
     * @return Metric description if it exists and loaded it the tree, otherwise null
     */
    public MetricDescription maybeFindMetric(String[] levels) {
        MetricDir dir = root;
        int lastLevel = levels.length - 1;
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            if (i == lastLevel) {
                return dir.maybeGetMetric(level);
            } else {
                dir = dir.maybeGetDir(level);
                if (dir == null || dir.getStatus() == MetricStatus.BAN) {
                    return null;
                }
            }
        }
        throw new IllegalStateException();
    }

    /**
     * Tries to find parent dir for a metric in in-memory cache. Will return dirs with any status, including
     * {@link MetricStatus#BAN}, unlike {@link #maybeFindMetric}.
     *
     * @param metricLevels metric to find parent for.
     * @return parent dir or null if parent dir isn't in in-memory cache.
     */
    public MetricDir maybeFindParent(String[] metricLevels) {
        MetricDir currentDir = root;
        int currentLevelIndex = 0;
        while (currentLevelIndex < metricLevels.length - 1 && currentDir != null) {
            currentDir = currentDir.maybeGetDir(metricLevels[currentLevelIndex]);
            currentLevelIndex++;
        }
        return currentDir;
    }

    /**
     * Creates or changes the status of a metric or an entire directory
     *
     * @param metric if ends with ".", then it's a directory
     * @param status
     * @return MetricDescription, or <code>null</code> if the metric/directory is banned, or limit reached
     */
    public MetricDescription modify(String metric, MetricStatus status) {
        boolean isDir = MetricUtil.isDir(metric);

        String[] levels = MetricUtil.splitToLevels(metric);

        MetricDir dir = root;
        for (int i = 0; i < levels.length; i++) {
            boolean isLast = (i == levels.length - 1);
            if (dir == null || dir.getStatus() == MetricStatus.BAN) {
                return null;
            }
            String level = levels[i];
            if (!isLast) {
                dir = dir.getOrCreateDir(level, status, metricDirFactory, maxSubDirsPerDir);
            } else {
                MetricBase metricBase;
                if (isDir) {
                    metricBase = dir.getOrCreateDir(level, status, metricDirFactory, maxSubDirsPerDir);
                } else {
                    metricBase = dir.getOrCreateMetric(level, status, retentionProvider, maxMetricsPerDir);
                }
                if (metricBase != null) {
                    metricBase.setStatus(MetricStatus.selectStatus(metricBase.getStatus(), status));
                }
                return metricBase;
            }
        }
        throw new IllegalStateException();
    }

    @VisibleForTesting
    static boolean containsExpressions(String metric) {
        return EXPRESSION_MATCHER.matchesAnyOf(metric);
    }


}
