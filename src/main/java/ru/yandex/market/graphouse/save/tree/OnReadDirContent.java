package ru.yandex.market.graphouse.save.tree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

public class OnReadDirContent {
    private static final Logger log = LogManager.getLogger();

    private final OnReadDirContentBatcher dirContentBatcher;
    private volatile ConcurrentMap<String, OnRecordMetricDescription> dirs = null;
    private volatile ConcurrentMap<String, OnRecordMetricDescription> metrics = null;
    private volatile Future<OnReadDirContent> loadFuture = null;
    private volatile boolean loaded;

    public OnReadDirContent() {
        this(null);
    }

    public OnReadDirContent(OnReadDirContentBatcher dirContentBatcher) {
        this.dirContentBatcher = dirContentBatcher;
        this.loaded = dirContentBatcher == null;
    }

    public OnRecordMetricDescription getDescription(String name, boolean dir) {
        ConcurrentMap<String, OnRecordMetricDescription> content = getContent(dir);
        return content == null ? null : content.get(name);
    }

    public int getContentCount(boolean dir) {
        ConcurrentMap<String, OnRecordMetricDescription> content = getContent(dir);
        return content == null ? 0 : content.size();
    }

    public void removeDescription(String name, boolean dir) {
        ConcurrentMap<String, OnRecordMetricDescription> content = getContent(dir);
        if (content != null) {
            content.remove(name);
        }
    }

    public OnRecordMetricDescription computeDescriptionIfAbsent(
        String name,
        boolean dir,
        Function<String, OnRecordMetricDescription> descriptionCreator
    ) {
        ConcurrentMap<String, OnRecordMetricDescription> content = getOrInitContent(dir);
        return content.computeIfAbsent(name, descriptionCreator);
    }

    private ConcurrentMap<String, OnRecordMetricDescription> getContent(boolean dir) {
        return dir ? dirs : metrics;
    }

    private ConcurrentMap<String, OnRecordMetricDescription> getOrInitContent(boolean dir) {
        if (dir) {
            if (dirs == null) {
                initDirContent();
            }
            return dirs;
        } else {
            if (metrics == null) {
                initMetrics();
            }
            return metrics;
        }
    }

    private synchronized void initDirContent() {
        if (dirs == null) {
            dirs = new ConcurrentHashMap<>();
        }
    }

    private synchronized void initMetrics() {
        if (metrics == null) {
            metrics = new ConcurrentHashMap<>();
        }
    }


    public static OnReadDirContent createEmpty() {
        return new OnReadDirContent();
    }

    public static OnReadDirContent createEmpty(
        OnReadDirContentBatcher dirContentBatcher
    ) {
        return new OnReadDirContent(dirContentBatcher);
    }

    public boolean isLoaded() {
        return loaded;
    }

    public void setLoaded(boolean loaded) {
        this.loaded = loaded;
    }

    public OnReadDirContent tryToLoad(OnRecordMetricDescription metricDescription) {
        if (loaded) {
            return this;
        }

        return lazyLoadDirContent(metricDescription);
    }

    private synchronized OnReadDirContent lazyLoadDirContent(OnRecordMetricDescription metricDescription) {
        if (loaded) {
            return this;
        }

        if (loadFuture == null) {
            loadFuture = dirContentBatcher.lazyLoadDirContent(metricDescription, this);
        }

        if (loadFuture == null) {
            log.warn("Batch for load is not created");
            return this;
        }

        try {
            if (loadFuture.isDone()) {
                OnReadDirContent loadedDirContent = loadFuture.get();

                if (loadedDirContent == null) {
                    log.warn("Loaded dir content is null for '{}'", metricDescription.getName());
                    loaded = false;
                    loadFuture = null;
                    return this;
                }

                if (loadedDirContent != this) {
                    log.warn("Loaded content is not equals this: '{}'", metricDescription.getName());
                }
                loaded = true;
                loadFuture = null;
                return loadedDirContent;
            }
        } catch (ExecutionException e) {
            log.error("Error on load DirContent", e);
            loaded = false;
            loadFuture = null;
        } catch (InterruptedException ignore) {
        }

        return this;
    }
}
