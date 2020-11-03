package ru.yandex.market.graphouse.search.tree;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 25/01/2017
 */
public class LoadableMetricDir extends MetricDir {
    private final AsyncLoadingCache<MetricDir, DirContent> dirContentProvider;

    public LoadableMetricDir(MetricDir parent, String name, MetricStatus status,
                             AsyncLoadingCache<MetricDir, DirContent> dirContentProvider) {
        super(parent, name, status);
        this.dirContentProvider = dirContentProvider;
    }

    private DirContent getContent() {
        try {
            return dirContentProvider.get(this).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    private DirContent getContentOrEmpty() {
        CompletableFuture<DirContent> content = dirContentProvider.getIfPresent(this);
        if (content == null) {
            return DirContent.EMPTY;
        }
        DirContent dirContent = content.getNow(DirContent.EMPTY);
        return (dirContent == null) ? DirContent.EMPTY : dirContent;
    }

    @Override
    public Map<String, MetricDir> getDirs() {
        return getContent().getDirs();
    }

    @Override
    public Map<String, MetricName> getMetrics() {
        return getContent().getMetrics();
    }

    @Override
    public boolean hasDirs() {
        return !getDirs().isEmpty();
    }

    @Override
    public boolean hasMetrics() {
        return !getMetrics().isEmpty();
    }

    @Override
    public MetricDir maybeGetDir(String name) {
        return getContentOrEmpty().getDirs().get(name);
    }

    @Override
    public MetricName maybeGetMetric(String name) {
        return getContentOrEmpty().getMetrics().get(name);
    }

    @Override
    public int loadedMetricCount() {
        DirContent dirContent = getContentOrEmpty();
        int count = dirContent.getMetrics().size();
        for (MetricDir metricDir : dirContent.getDirs().values()) {
            count += metricDir.loadedMetricCount();
        }
        return count;
    }

    @Override
    public int loadedDirCount() {
        DirContent dirContent = getContentOrEmpty();
        int count = dirContent.getDirs().size();
        for (MetricDir metricDir : dirContent.getDirs().values()) {
            count += metricDir.loadedDirCount();
        }
        return count;
    }
}
