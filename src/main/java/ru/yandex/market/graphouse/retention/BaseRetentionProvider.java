package ru.yandex.market.graphouse.retention;

import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 04/04/2017
 */
public class BaseRetentionProvider implements RetentionProvider {
    private final List<MetricRetentionConfig> retentions;

    public BaseRetentionProvider(List<MetricRetentionConfig> retentions) {
        this.retentions = retentions;
    }

    @Override
    public MetricRetention getRetention(String metric) {
        for (MetricRetentionConfig metricRetentionConfig : retentions) {
            if (metricRetentionConfig.matches(metric)) {
                return metricRetentionConfig.getMetricRetention();
            }
        }
        throw new IllegalStateException("Retention for metric '" + metric + "' not found");
    }


}
