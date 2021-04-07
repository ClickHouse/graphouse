package ru.yandex.market.graphouse.save.banned;

import com.google.common.annotations.VisibleForTesting;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.utils.OnReadAppendableResult;

import java.io.IOException;

public class BannedMetricCache {
    private final CachedMetric root;

    public BannedMetricCache() {
        root = new CachedMetric(null, "ROOT", true, MetricStatus.SIMPLE);
    }

    public boolean isBanned(String name) {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        CachedMetric currentMetric = root;
        for (int i = 0; i < nameSplits.length; i++) {
            String metricName = nameSplits[i];
            boolean dir = i < nameSplits.length - 1 || MetricUtil.isDir(name);

            currentMetric = currentMetric.getChild(metricName, dir);
            if (currentMetric == null) {
                return false;
            }

            MetricStatus currentStatus = currentMetric.getStatus();
            if (MetricStatus.BAN == currentStatus || MetricStatus.AUTO_BAN == currentStatus) {
                return true;
            }
        }
        return false;
    }

    public void traceMetricStateInCache(String name, OnReadAppendableResult result) throws IOException {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        StringBuilder fullMetricName = new StringBuilder();
        CachedMetric currentMetric = root;
        for (int i = 0; i < nameSplits.length; i++) {
            String metricName = nameSplits[i];
            boolean dir = i < nameSplits.length - 1 || MetricUtil.isDir(name);

            fullMetricName.append(metricName);

            currentMetric = currentMetric.getChild(metricName, dir);
            if (currentMetric == null) {
                break;
            }

            MetricStatus metricStatus = currentMetric.getStatus();
            if (MetricStatus.SIMPLE != metricStatus) {
                result.appendNotSimpleStatus(fullMetricName.toString(), metricStatus);
            }
        }
    }

    public void addMetricWithStatus(String name, MetricStatus status) {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        CachedMetric currentMetric = root;
        for (int i = 0; i < nameSplits.length; i++) {
            String internMetricName = nameSplits[i].intern();
            boolean dir = i < nameSplits.length - 1 || MetricUtil.isDir(name);
            if (i < nameSplits.length - 1) {
                currentMetric = currentMetric.getOrCreateChild(internMetricName, dir, MetricStatus.SIMPLE);
            } else {
                currentMetric = currentMetric.updateOrCreateChild(internMetricName, dir, status);
            }
        }
    }

    public void resetBanStatus(String name, MetricStatus newStatus) {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        CachedMetric parentMetric = null;
        CachedMetric currentMetric = root;
        for (int i = 0; i < nameSplits.length; i++) {
            String metricName = nameSplits[i];
            boolean dir = i < nameSplits.length - 1 || MetricUtil.isDir(name);
            parentMetric = currentMetric;
            currentMetric = currentMetric.getChild(metricName, dir);

            if (currentMetric == null && i < nameSplits.length - 1) {
                return;
            }
        }

        if (currentMetric != null) {
            currentMetric.resetBanStatus(newStatus);
        }

        if (parentMetric != null) {
            parentMetric.resetAutoBanStatus(newStatus);
        }
    }

    public String printCacheState() {
        return root.getCacheState().toString();
    }

    @VisibleForTesting
    CacheState getCacheState() {
        return root.getCacheState();
    }

    @VisibleForTesting
    static class CacheState {
        public int nodesCount = 0;
        public int bannedMetricsCount = 0;
        public int autoBannedMetricsCount = 0;

        @Override
        public String toString() {
            return "Total nodes = " + nodesCount + ". Banned metrics count = " + bannedMetricsCount +
                ". AutoBanned metrics count = " + autoBannedMetricsCount;
        }
    }
}
