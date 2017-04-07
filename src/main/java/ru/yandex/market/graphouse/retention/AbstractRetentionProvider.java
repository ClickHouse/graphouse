package ru.yandex.market.graphouse.retention;

import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 04/04/2017
 */
public class AbstractRetentionProvider implements RetentionProvider{
    private final List<MetricRetention> retentions;

    public AbstractRetentionProvider(List<MetricRetention> retentions) {
        this.retentions = retentions;
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



}
