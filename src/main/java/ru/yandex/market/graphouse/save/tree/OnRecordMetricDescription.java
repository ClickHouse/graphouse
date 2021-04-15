package ru.yandex.market.graphouse.save.tree;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.UncheckedExecutionException;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.MetricBase;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class OnRecordMetricDescription implements MetricDescription {
    private static final String ROOT_NAME = "ROOT";
    private final AsyncLoadingCache<OnRecordMetricDescription, OnReadDirContent> dirContentProvider;

    private volatile OnRecordMetricDescription parent;
    private final String name;
    private volatile MetricStatus status;
    private final boolean dir;
    private volatile long updateTimeMillis;
    private volatile boolean maybeNewMetrics;
    private volatile boolean contentLoaded;

    public OnRecordMetricDescription(
        AsyncLoadingCache<OnRecordMetricDescription, OnReadDirContent> dirContentProvider,
        OnRecordMetricDescription parent,
        String name,
        MetricStatus status,
        boolean dir
    ) {
        this.dirContentProvider = dirContentProvider;
        this.parent = parent;
        this.name = name;
        this.status = status;
        this.dir = dir;
        this.updateTimeMillis = System.currentTimeMillis();
        this.maybeNewMetrics = true;
        this.contentLoaded = false;
    }

    @Override
    public String getName() {
        if (isRoot()) {
            return ROOT_NAME;
        }

        String currentName = name;
        if (isDir()) {
            currentName += MetricUtil.LEVEL_SPLITTER;
        }

        return parent.isRoot() ? currentName : parent.getName() + currentName;
    }

    @Override
    public int getNameLengthInBytes() {
        int length = name.getBytes().length;
        if (isDir()) {
            length++;
        }
        if (!parent.isRoot()) {
            length += parent.getNameLengthInBytes();
        }
        return length;
    }

    @Override
    public void writeName(ClickHouseRowBinaryStream stream) throws IOException {
        if (!parent.isRoot()) {
            parent.writeName(stream);
        }
        stream.writeBytes(name.getBytes());
        if (isDir()) {
            stream.writeByte(MetricBase.LEVEL_SPLITTER);
        }
    }

    public void setParent(OnRecordMetricDescription parent) {
        this.parent = parent;
    }

    @Override
    public MetricStatus getStatus() {
        return status;
    }

    public void setStatus(MetricStatus newStatus, boolean forceUpdate) {
        if (this.status == newStatus) {
            return;
        }

        if (forceUpdate) {
            this.status = newStatus;
        } else {
            MetricStatus nextStatus = MetricStatus.selectStatus(this.status, newStatus);

            if (!isMaybeNewMetrics()) {
                this.maybeNewMetrics = this.status != nextStatus;
            }
            this.status = nextStatus;
        }
    }

    @Override
    public boolean isDir() {
        return dir;
    }

    @Override
    public long getUpdateTimeMillis() {
        return updateTimeMillis;
    }

    public void setUpdateTimeMillis(long updateTimeMillis) {
        this.updateTimeMillis = updateTimeMillis;
    }

    @Override
    public OnRecordMetricDescription getParent() {
        return parent;
    }

    @Override
    public int getLevel() {
        int level = 0;
        MetricDescription metricBase = this;
        while (!metricBase.isRoot()) {
            level++;
            metricBase = metricBase.getParent();
        }
        return level;
    }

    @Override
    public boolean isRoot() {
        return parent == null;
    }

    public boolean isMaybeNewMetrics() {
        return maybeNewMetrics;
    }

    public void setMaybeNewMetrics(boolean maybeNewMetrics) {
        this.maybeNewMetrics = maybeNewMetrics;
    }

    @Override
    public String toString() {
        return getName();
    }

    @VisibleForTesting
    OnReadDirContent getContent() {
        try {
            OnReadDirContent dirContent = dirContentProvider.get(this).get();
            return tryToLoadContent(dirContent);
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    private OnReadDirContent tryToLoadContent(OnReadDirContent dirContent) {
        OnReadDirContent loadedDirContent = dirContent;
        if (dirContent.isLoaded()) {
            contentLoaded = true;
            return dirContent;
        } else {
            if (isRoot() || parent.isContentLoaded()) {
                if (isMaybeNewMetrics()) {
                    contentLoaded = true;
                    dirContent.setLoaded(true);
                    return dirContent;
                } else {
                    contentLoaded = false;
                    loadedDirContent = dirContent.tryToLoad(this);
                }
            }
            contentLoaded = loadedDirContent.isLoaded();
            return loadedDirContent;
        }
    }

    public OnReadDirContent getContentIfExists() {
        CompletableFuture<OnReadDirContent> dirContentFuture = dirContentProvider.getIfPresent(this);
        if (dirContentFuture == null) {
            return null;
        }
        return dirContentFuture.getNow(null);
    }

    public OnRecordMetricDescription getOrCreateDir(
        String name,
        int maxSubDirsPerDir
    ) {
        return getMetricDescription(name, maxSubDirsPerDir, true);
    }

    public OnRecordMetricDescription getOrCreateMetric(
        String name,
        int maxMetricsPerDir
    ) {
        return getMetricDescription(name, maxMetricsPerDir, false);
    }


    private OnRecordMetricDescription getMetricDescription(
        String name,
        int maxMetricsPerDir,
        boolean dir
    ) {
        OnReadDirContent content = getContent();
        OnRecordMetricDescription metricDescription = content.getDescription(name, dir);
        if (metricDescription != null) {
            return metricDescription;
        }
        if (maxMetricsPerDir > 0 && content.getContentCount(dir) >= maxMetricsPerDir) {
            return null;
        }
        String internName = name.intern();
        metricDescription = content.computeDescriptionIfAbsent(
            internName, dir,
            s -> new OnRecordMetricDescription(
                dirContentProvider, OnRecordMetricDescription.this, internName, MetricStatus.SIMPLE, dir
            )
        );
        return metricDescription;
    }

    public boolean isAutoBannedForDirs(int maxSubDirsPerDir) {
        if (maxSubDirsPerDir <= 0 || this.status.handmade()) {
            return false;
        }
        OnReadDirContent dirContent = getContentIfExists();
        if (dirContent == null) {
            return false;
        }
        return dirContent.getContentCount(true) >= maxSubDirsPerDir;
    }

    public boolean isAutoBannedForMetrics(int maxMetricsPerDir) {
        if (maxMetricsPerDir <= 0 || this.status.handmade()) {
            return false;
        }
        OnReadDirContent dirContent = getContentIfExists();
        if (dirContent == null) {
            return false;
        }
        return dirContent.getContentCount(false) >= maxMetricsPerDir;
    }

    public boolean isContentLoaded() {
        return contentLoaded;
    }

    public void setContentLoaded(boolean contentLoaded) {
        this.contentLoaded = contentLoaded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OnRecordMetricDescription that = (OnRecordMetricDescription) o;

        return isEquals(this, that);
    }

    private static boolean isEquals(OnRecordMetricDescription d1, OnRecordMetricDescription d2) {
        while (d1 != null && d2 != null) {
            if (d1 == d2) {
                return true;
            }
            if (!d1.name.equals(d2.name)) {
                return false;
            }
            d1 = d1.getParent();
            d2 = d2.getParent();
        }
        return d1 == null && d2 == null;
    }

    @Override
    public int hashCode() {
        if (isRoot()) {
            return 0;
        }
        return 31 * getParent().hashCode() + name.hashCode();
    }
}
