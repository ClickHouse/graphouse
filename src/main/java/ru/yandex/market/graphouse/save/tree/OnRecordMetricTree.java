package ru.yandex.market.graphouse.save.tree;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.save.UpdateMetricQueueService;
import ru.yandex.market.graphouse.save.banned.BannedMetricCache;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.utils.OnReadAppendableResult;

import java.io.IOException;

public class OnRecordMetricTree {
    private static final Logger log = LogManager.getLogger();

    private final UpdateMetricQueueService updateMetricQueue;
    private final BannedMetricCache metricCache;
    private final OnRecordMetricDescription root;
    private final int maxSubDirsPerDir;
    private final int maxMetricsPerDir;

    public OnRecordMetricTree(
        UpdateMetricQueueService updateMetricQueue,
        BannedMetricCache metricCache,
        AsyncLoadingCache<OnRecordMetricDescription, OnReadDirContent> dirContentProvider,
        int maxSubDirsPerDir,
        int maxMetricsPerDir
    ) {
        this.updateMetricQueue = updateMetricQueue;
        this.metricCache = metricCache;
        this.maxSubDirsPerDir = maxSubDirsPerDir;
        this.maxMetricsPerDir = maxMetricsPerDir;

        root = new OnRecordMetricDescription(dirContentProvider, null, "", MetricStatus.SIMPLE, true);
    }

    public OnRecordMetricDescription getOrCreateMetric(String name) {
        boolean isDir = MetricUtil.isDir(name);

        String[] nameSplits = MetricUtil.splitToLevels(name);
        OnRecordMetricDescription currentDir = root;
        for (int i = 0; i < nameSplits.length; i++) {
            String nameOnLevel = nameSplits[i];
            if (i < nameSplits.length - 1) {
                OnRecordMetricDescription nextDir = currentDir.getOrCreateDir(nameOnLevel, maxSubDirsPerDir);
                if (nextDir == null) {
                    if (currentDir.isAutoBannedForDirs(maxSubDirsPerDir)) {
                        metricCache.addMetricWithStatus(currentDir.getName(), MetricStatus.AUTO_BAN);
                    } else {
                        log.warn("Simple dir is not created {}{}.", currentDir.getName(), nameOnLevel);
                    }
                    return null;
                } else {
                    nextDir.setStatus(MetricStatus.SIMPLE, false);
                    updateIfNewMetric(currentDir, nextDir);
                }
                currentDir = nextDir;
            } else {
                OnRecordMetricDescription metric = isDir
                    ? currentDir.getOrCreateDir(nameOnLevel, maxSubDirsPerDir)
                    : currentDir.getOrCreateMetric(nameOnLevel, maxMetricsPerDir);
                if (metric == null) {
                    if (currentDir.isAutoBannedForMetrics(maxMetricsPerDir)) {
                        metricCache.addMetricWithStatus(currentDir.getName(), MetricStatus.AUTO_BAN);
                    } else {
                        log.warn("Simple metric is not created {}{}", currentDir.getName(), nameOnLevel);
                    }
                    return null;
                } else {
                    metric.setStatus(MetricStatus.SIMPLE, false);
                    updateIfNewMetric(currentDir, metric);
                }
                return metric;
            }
        }
        return null;
    }

    private void updateIfNewMetric(OnRecordMetricDescription dir, OnRecordMetricDescription child) {
        if (dir.isContentLoaded() && child.isMaybeNewMetrics()) {
            child.setUpdateTimeMillis(System.currentTimeMillis());
            updateMetricQueue.addUpdatedMetrics(child);
            child.setMaybeNewMetrics(false);
            child.setContentLoaded(true);
        }
    }

    public void traceMetricStateInCache(String name, OnReadAppendableResult onReadAppendableResult) throws IOException {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        OnRecordMetricDescription currentDescription = root;
        for (int i = 0; i < nameSplits.length; i++) {
            String nameOnLevel = nameSplits[i];
            boolean dir = i < nameSplits.length - 1 || MetricUtil.isDir(name);

            OnReadDirContent content = currentDescription.getContentIfExists();

            if (content != null) {
                currentDescription = content.getDescription(nameOnLevel, dir);
            } else {
                return;
            }

            onReadAppendableResult.appendMetric(currentDescription, content);

            if (currentDescription == null) {
                return;
            }
        }
    }

    public OnRecordMetricDescription tryToFindMetric(String name) {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        OnRecordMetricDescription currentDescription = root;
        for (int i = 0; i < nameSplits.length; i++) {
            String nameOnLevel = nameSplits[i];
            boolean dir = i < nameSplits.length - 1 || MetricUtil.isDir(name);

            OnReadDirContent dirContent = currentDescription.getContentIfExists();
            if (dirContent == null) {
                return null;
            } else {
                currentDescription = dirContent.getDescription(nameOnLevel, dir);
            }

            if (currentDescription == null) {
                return null;
            }
        }
        return currentDescription;
    }

    public void updateMetricIfLoaded(String name, MetricStatus status) {
        String parentName = MetricUtil.getParentName(name);
        OnRecordMetricDescription parentDir = tryToFindMetric(parentName);

        if (parentDir == null) {
            return;
        }

        String metricName = MetricUtil.getLastLevelName(name);
        OnRecordMetricDescription metricDescription;

        if (MetricUtil.isDir(name)) {
            metricDescription = parentDir.getOrCreateDir(metricName, status.handmade() ? -1 : maxSubDirsPerDir);
        } else {
            metricDescription = parentDir.getOrCreateMetric(metricName, status.handmade() ? -1 : maxMetricsPerDir);
        }

        if (metricDescription != null) {
            metricDescription.setMaybeNewMetrics(false);
            metricDescription.setStatus(status, true);
        }
    }

    public void removeMetricFromTree(String name) {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        OnRecordMetricDescription currentDescription = root;
        OnReadDirContent dirContent = null;
        String nameOnLevel = "";

        for (int i = 0; i < nameSplits.length; i++) {
            nameOnLevel = nameSplits[i];
            boolean dir = i < nameSplits.length - 1 || MetricUtil.isDir(name);

            dirContent = currentDescription.getContentIfExists();
            if (dirContent == null) {
                return;
            } else {
                currentDescription = dirContent.getDescription(nameOnLevel, dir);
            }

            if (currentDescription == null) {
                return;
            }
        }

        if (dirContent != null) {
            dirContent.removeDescription(nameOnLevel, MetricUtil.isDir(name));
        }

        currentDescription.setMaybeNewMetrics(false);
    }
}
