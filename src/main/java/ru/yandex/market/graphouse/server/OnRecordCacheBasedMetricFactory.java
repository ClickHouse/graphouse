package ru.yandex.market.graphouse.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.save.OnRecordMetricProvider;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.regex.Pattern;

public class OnRecordCacheBasedMetricFactory extends BaseMetricFactory {
    private static final Logger log = LogManager.getLogger();
    private final OnRecordMetricProvider onRecordMetricProvider;
    private final SearchCacheBasedMetricFactory searchCacheBasedMetricFactory;
    private final boolean cacheEnable;
    private final Pattern directoriesForSearchCache;

    public OnRecordCacheBasedMetricFactory(
        MetricValidator metricValidator,
        boolean redirectHostMetrics,
        String hostMetricDir,
        String hostPostfixes,
        SearchCacheBasedMetricFactory searchCacheBasedMetricFactory,
        OnRecordMetricProvider onRecordMetricProvider,
        boolean cacheEnable,
        String directoriesForSearchCache
    ) {
        super(metricValidator, redirectHostMetrics, hostMetricDir, hostPostfixes);
        this.searchCacheBasedMetricFactory = searchCacheBasedMetricFactory;
        this.onRecordMetricProvider = onRecordMetricProvider;
        this.cacheEnable = cacheEnable;
        this.directoriesForSearchCache = createDirectoriesPattern(directoriesForSearchCache);
    }

    private Pattern createDirectoriesPattern(String directoriesForSearchCache) {
        if (directoriesForSearchCache != null && !directoriesForSearchCache.isEmpty()) {
            return MetricUtil.createStartWithDirectoryPattern(
                directoriesForSearchCache.split(",")
            );
        }
        return null;
    }

    @Override
    protected MetricDescription getOrCreateMetricDescription(String name) {
        if (cacheEnable) {
            if (!metricValidator.validate(name, false)) {
                return null;
            }
            return getMetricDescription(name);
        } else {
            throw new RuntimeException("OnReadCache disabled");
        }
    }

    private MetricDescription getMetricDescription(String name) {
        if (directoriesForSearchCache == null || !directoriesForSearchCache.matcher(name).find()) {
            return onRecordMetricProvider.getMetricDescription(name);
        } else {
            return searchCacheBasedMetricFactory.getOrCreateMetricDescription(name);
        }
    }

    @Override
    protected Logger getLog() {
        return log;
    }
}
