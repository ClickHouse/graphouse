package ru.yandex.market.graphouse.retention;

import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 04/04/2017
 */
public class AbstractRetentionProvider implements RetentionProvider {
    private final List<MetricRetention> retentions;
    private final MetricRetention defaultRetention;

    public AbstractRetentionProvider(List<MetricRetention> retentions) {
        this(retentions, retentions.get(retentions.size() - 1));
    }

    public AbstractRetentionProvider(List<MetricRetention> retentions, MetricRetention defaultRetention) {
        this.retentions = retentions;
        this.defaultRetention = defaultRetention;
    }

    @Override
    public MetricRetention getRetention(String metric) {
        for (MetricRetention metricRetention : retentions) {
            if (metricRetention.matches(metric)) {
                return metricRetention;
            }
        }
        throw new IllegalStateException("Retention for metric '" + metric + "' not found");
    }

    @Override
    public MetricRetention getDefaultRetention() {
        return defaultRetention;
    }
}
