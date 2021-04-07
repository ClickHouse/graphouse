package ru.yandex.market.graphouse.save.banned;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.util.concurrent.ConcurrentHashMap;

class CachedMetric {
    private static final Logger log = LogManager.getLogger();
    private final CachedMetric parent;
    private final String name;
    private final boolean directory;
    private volatile MetricStatus status;
    private volatile ConcurrentHashMap<String, CachedMetric> dirs;
    private volatile ConcurrentHashMap<String, CachedMetric> metrics;

    public CachedMetric(CachedMetric parent, String name, boolean directory, MetricStatus status) {
        this.parent = parent;
        this.name = name;
        this.status = status;
        this.directory = directory;
        this.dirs = null;
        this.metrics = null;
    }

    public MetricStatus getStatus() {
        return status;
    }

    public void setStatus(MetricStatus status) {
        this.status = status;
    }

    public CachedMetric getParent() {
        return parent;
    }

    public CachedMetric getChild(String name, boolean dir) {
        ConcurrentHashMap<String, CachedMetric> children = dir ? dirs : metrics;
        if (children == null) {
            return null;
        }
        return children.get(name);
    }

    public CachedMetric getOrCreateChild(String name, boolean dir, MetricStatus status) {
        return getInitializedChildren(dir).computeIfAbsent(name, n -> createChild(n, dir, status));
    }

    public CachedMetric updateOrCreateChild(String name, boolean dir, MetricStatus status) {
        CachedMetric child = getInitializedChildren(dir).computeIfAbsent(name, n -> createChild(n, dir, status));
        child.setStatus(status);
        return child;
    }

    private ConcurrentHashMap<String, CachedMetric> getInitializedChildren(boolean dir) {
        if (dir) {
            if (dirs == null) {
                initDirs();
            }
            return dirs;
        } else {
            if (metrics == null) {
                initMetrics();
            }
            return metrics;
        }
    }

    private synchronized void initDirs() {
        if (dirs == null) {
            dirs = new ConcurrentHashMap<>();
        }
    }

    private synchronized void initMetrics() {
        if (metrics == null) {
            metrics = new ConcurrentHashMap<>();
        }
    }

    private CachedMetric createChild(String name, boolean dir, MetricStatus status) {
        return new CachedMetric(this, name, dir, status);
    }

    public void resetBanStatus(MetricStatus newStatus) {
        if (MetricStatus.SIMPLE != newStatus || MetricStatus.AUTO_BAN != status) {
            this.status = MetricStatus.SIMPLE;
            removeFromParentIfNeed();
        }
    }

    public void resetAutoBanStatus(MetricStatus newChildStatus) {
        if (newChildStatus.handmade() && MetricStatus.AUTO_BAN == status) {
            this.status = MetricStatus.SIMPLE;
            removeFromParentIfNeed();
        }
    }

    private void removeFromParentIfNeed() {
        if (!isRoot() && isChildrenEmpty() && MetricStatus.SIMPLE == this.status) {
            parent.removeChild(this.name, this.directory);
        }
    }

    private boolean isChildrenEmpty() {
        return (dirs == null || dirs.isEmpty()) && (metrics == null || metrics.isEmpty());
    }

    private void removeChild(String childName, boolean dir) {
        ConcurrentHashMap<String, CachedMetric> children = dir ? dirs : metrics;
        if (children == null) {
            return;
        }
        CachedMetric child = children.get(childName);
        if (child != null) {
            log.info(
                "Removed metric '{}' with state '{}' from banned cache",
                child.getFullName(), child.getStatus()
            );
            children.remove(childName);

            removeFromParentIfNeed();
        }
    }

    public String getFullName() {
        if (isRoot()) {
            return name;
        }

        return parent.isRoot() ? name : parent.getFullName() + name;
    }

    private boolean isRoot() {
        return parent == null;
    }

    public BannedMetricCache.CacheState getCacheState() {
        BannedMetricCache.CacheState cacheState = new BannedMetricCache.CacheState();
        fillCacheState(cacheState);
        return cacheState;
    }

    private void fillCacheState(BannedMetricCache.CacheState cacheState) {
        switch (status) {
            case BAN:
                cacheState.bannedMetricsCount++;
                break;
            case AUTO_BAN:
                cacheState.autoBannedMetricsCount++;
                break;
        }

        if (dirs != null) {
            cacheState.nodesCount += dirs.size();
            dirs.values().forEach(c -> c.fillCacheState(cacheState));
        }

        if (metrics != null) {
            cacheState.nodesCount += metrics.size();
            metrics.values().forEach(c -> c.fillCacheState(cacheState));
        }
    }
}
