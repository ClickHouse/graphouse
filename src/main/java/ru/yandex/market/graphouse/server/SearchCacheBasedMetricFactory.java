package ru.yandex.market.graphouse.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/05/15
 */
public class SearchCacheBasedMetricFactory extends BaseMetricFactory {
    private static final Logger log = LogManager.getLogger();

    private final MetricSearch metricSearch;

    public SearchCacheBasedMetricFactory(
        MetricSearch metricSearch, MetricValidator metricValidator,
        boolean redirectHostMetrics, String hostMetricDir, String hostPostfixes
    ) {
        super(metricValidator, redirectHostMetrics, hostMetricDir, hostPostfixes);
        this.metricSearch = metricSearch;
    }

    @Override
    protected MetricDescription getOrCreateMetricDescription(String name) {
        String[] nameSplits = MetricUtil.splitToLevels(name);
        //Trying to fast find metric in tree. In success we can skip validation;
        MetricDescription metric = metricSearch.maybeFindMetric(nameSplits);
        if (metric == null) {
            if (!metricValidator.validate(name, false)) {
                return null;
            }
            metric = metricSearch.add(name);
            if (metric == null) {
                return null;
            }
        } else if (metric.getStatus() == MetricStatus.AUTO_HIDDEN || metric.getStatus() == MetricStatus.HIDDEN) {
            metric = metricSearch.add(name);
        }
        if (metric.getStatus() == MetricStatus.BAN) {
            return null;
        }
        return metric;
    }

    @Override
    protected Logger getLog() {
        return log;
    }
}
